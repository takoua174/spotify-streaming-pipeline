"""
kmeans_genre_clustering.py
==========================
Batch Spark job for musical similarity clustering.

Flow:
1. Read song metadata from Kafka topic song-metadata.
2. Evaluate candidate K values with silhouette and training cost.
3. Train final KMeans model.
4. Persist outputs to PostgreSQL and publish cluster signals to Kafka topic genre-signals.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

FEATURE_COLUMNS = [
    "danceability",
    "energy",
    "acousticness",
    "instrumentalness",
    "valence",
    "speechiness",
    "liveness",
    "loudness_norm",
    "tempo_norm",
]


def load_env_file(path: Path) -> None:
    """Load .env values without external dependency."""
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass
class AppConfig:
    spark_app_name: str
    spark_jars_ivy: str
    spark_sql_shuffle_partitions: int
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_starting_offsets: str
    kafka_ending_offsets: str
    input_max_rows: int | None
    candidate_ks: list[int]
    final_k: int | None
    kmeans_max_iter: int
    kmeans_seed: int
    kmeans_tol: float
    k_selection_sample_fraction: float
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    table_batch_runs: str
    table_cluster_metrics: str
    table_song_clusters: str
    table_cluster_centroids: str
    table_cluster_profiles: str
    run_id: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


def parse_int_list(raw_value: str, fallback: list[int]) -> list[int]:
    values: list[int] = []
    for token in raw_value.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            value = int(token)
            if value > 1:
                values.append(value)
        except ValueError:
            continue

    values = sorted(set(values))
    return values or fallback


def parse_optional_int(raw_value: str) -> int | None:
    value = raw_value.strip()
    if not value:
        return None
    return int(value)


def load_config() -> AppConfig:
    project_root = Path(__file__).resolve().parents[2]
    load_env_file(project_root / ".env")

    run_id = os.getenv(
        "KMEANS_RUN_ID",
        datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ"),
    )

    return AppConfig(
        spark_app_name=os.getenv("SPARK_APP_NAME", "genre-kmeans-batch"),
        spark_jars_ivy=os.getenv("SPARK_JARS_IVY", "/tmp/.ivy2"),
        spark_sql_shuffle_partitions=int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200")),
        kafka_bootstrap_servers=os.getenv("SPARK_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        kafka_input_topic=os.getenv("SPARK_KAFKA_INPUT_TOPIC", os.getenv("TOPIC_SONG_METADATA", "song-metadata")),
        kafka_output_topic=os.getenv("SPARK_KAFKA_OUTPUT_TOPIC", os.getenv("TOPIC_GENRE_SIGNALS", "genre-signals")),
        kafka_starting_offsets=os.getenv("SPARK_KAFKA_STARTING_OFFSETS", "earliest"),
        kafka_ending_offsets=os.getenv("SPARK_KAFKA_ENDING_OFFSETS", "latest"),
        input_max_rows=parse_optional_int(os.getenv("KMEANS_INPUT_MAX_ROWS", "")),
        candidate_ks=parse_int_list(os.getenv("KMEANS_CANDIDATE_KS", "6,8,10,12"), [6, 8, 10, 12]),
        final_k=parse_optional_int(os.getenv("KMEANS_FINAL_K", "")),
        kmeans_max_iter=int(os.getenv("KMEANS_MAX_ITER", "30")),
        kmeans_seed=int(os.getenv("KMEANS_SEED", "42")),
        kmeans_tol=float(os.getenv("KMEANS_TOL", "0.0001")),
        k_selection_sample_fraction=float(os.getenv("KMEANS_SELECTION_SAMPLE_FRACTION", "1.0")),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_db=os.getenv("POSTGRES_DB", "spotify_analytics"),
        postgres_user=os.getenv("POSTGRES_USER", "spotify_user"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "spotify_pass"),
        postgres_schema=os.getenv("POSTGRES_SCHEMA", "analytics"),
        table_batch_runs=os.getenv("PG_TABLE_BATCH_RUNS", "batch_runs"),
        table_cluster_metrics=os.getenv("PG_TABLE_CLUSTER_METRICS", "cluster_metrics"),
        table_song_clusters=os.getenv("PG_TABLE_SONG_CLUSTERS", "song_clusters"),
        table_cluster_centroids=os.getenv("PG_TABLE_CLUSTER_CENTROIDS", "cluster_centroids"),
        table_cluster_profiles=os.getenv("PG_TABLE_CLUSTER_PROFILES", "cluster_profiles"),
        run_id=run_id,
    )


def build_spark_session(cfg: AppConfig) -> SparkSession:
    return (
        SparkSession.builder.appName(cfg.spark_app_name)
        .config("spark.jars.ivy", cfg.spark_jars_ivy)
        .config("spark.sql.shuffle.partitions", str(cfg.spark_sql_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def song_metadata_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("song_id", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("album_name", T.StringType(), True),
            T.StructField("artists", T.ArrayType(T.StringType()), True),
            T.StructField("primary_artist", T.StringType(), True),
            T.StructField("danceability", T.DoubleType(), True),
            T.StructField("energy", T.DoubleType(), True),
            T.StructField("key", T.IntegerType(), True),
            T.StructField("loudness", T.DoubleType(), True),
            T.StructField("mode", T.IntegerType(), True),
            T.StructField("speechiness", T.DoubleType(), True),
            T.StructField("acousticness", T.DoubleType(), True),
            T.StructField("instrumentalness", T.DoubleType(), True),
            T.StructField("liveness", T.DoubleType(), True),
            T.StructField("valence", T.DoubleType(), True),
            T.StructField("tempo", T.DoubleType(), True),
            T.StructField("duration_ms", T.LongType(), True),
            T.StructField("explicit", T.BooleanType(), True),
            T.StructField("artist_followers", T.LongType(), True),
            T.StructField("artist_popularity", T.LongType(), True),
            T.StructField("main_genre", T.StringType(), True),
            T.StructField("genres", T.ArrayType(T.StringType()), True),
            T.StructField("source", T.StringType(), True),
        ]
    )


def read_song_metadata(spark: SparkSession, cfg: AppConfig) -> tuple[int, DataFrame]:
    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_input_topic)
        .option("startingOffsets", cfg.kafka_starting_offsets)
        .option("endingOffsets", cfg.kafka_ending_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_df.select(F.col("value").cast("string").alias("payload_json"))
        .select(F.from_json(F.col("payload_json"), song_metadata_schema()).alias("payload"))
        .select("payload.*")
    )

    raw_count = parsed.count()

    flattened = parsed.select(
        F.col("song_id"),
        F.coalesce(F.col("main_genre"), F.lit("unknown")).alias("main_genre"),
        F.col("genres"),
        F.col("danceability").alias("danceability"),
        F.col("energy").alias("energy"),
        F.col("acousticness").alias("acousticness"),
        F.col("instrumentalness").alias("instrumentalness"),
        F.col("valence").alias("valence"),
        F.col("speechiness").alias("speechiness"),
        F.col("liveness").alias("liveness"),
        F.col("loudness").alias("loudness"),
        F.col("tempo").alias("tempo"),
    )

    cleaned = flattened.filter(F.col("song_id").isNotNull())

    for column in [
        "danceability",
        "energy",
        "acousticness",
        "instrumentalness",
        "valence",
        "speechiness",
        "liveness",
        "loudness",
        "tempo",
    ]:
        cleaned = cleaned.withColumn(column, F.col(column).cast("double"))

    cleaned = cleaned.withColumn(
        "loudness_norm",
        F.greatest(F.lit(0.0), F.least((F.col("loudness") + F.lit(60.0)) / F.lit(60.0), F.lit(1.0))),
    )
    cleaned = cleaned.withColumn(
        "tempo_norm",
        F.greatest(F.lit(0.0), F.least(F.col("tempo") / F.lit(250.0), F.lit(1.0))),
    )

    for column in FEATURE_COLUMNS:
        cleaned = cleaned.withColumn(column, F.greatest(F.lit(0.0), F.least(F.col(column), F.lit(1.0))))

    cleaned = cleaned.dropna(subset=FEATURE_COLUMNS)
    deduped = cleaned.dropDuplicates(["song_id"])
    if cfg.input_max_rows is not None and cfg.input_max_rows > 0:
        deduped = deduped.limit(cfg.input_max_rows)
    deduped = deduped.cache()
    return raw_count, deduped


def write_to_postgres(df: DataFrame, cfg: AppConfig, table_name: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", f"{cfg.postgres_schema}.{table_name}")
        .option("user", cfg.postgres_user)
        .option("password", cfg.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_to_kafka_messages(df: DataFrame, cfg: AppConfig) -> None:
    (
        df.select(F.col("key").cast("string"), F.col("value").cast("string"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("topic", cfg.kafka_output_topic)
        .save()
    )


def fit_kmeans(df: DataFrame, k: int, cfg: AppConfig) -> tuple[Any, DataFrame, float, float | None, int]:
    model = (
        KMeans(
            k=k,
            seed=cfg.kmeans_seed,
            maxIter=cfg.kmeans_max_iter,
            tol=cfg.kmeans_tol,
            featuresCol="features_vec",
            predictionCol="prediction",
        )
        .fit(df)
    )

    predictions = model.transform(df)
    evaluator = ClusteringEvaluator(
        predictionCol="prediction",
        featuresCol="features_vec",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )

    silhouette = float(evaluator.evaluate(predictions))

    training_cost: float | None
    try:
        training_cost = float(model.summary.trainingCost)
    except Exception:
        training_cost = None

    num_clusters_found = predictions.select("prediction").distinct().count()
    return model, predictions, silhouette, training_cost, num_clusters_found


def ensure_features_vector(df: DataFrame) -> DataFrame:
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features_vec")
    return assembler.transform(df)


def build_centroids_df(spark: SparkSession, cfg: AppConfig, model: Any) -> DataFrame:
    rows: list[dict[str, Any]] = []
    centers = model.clusterCenters()

    for cluster_id, center in enumerate(centers):
        row: dict[str, Any] = {
            "run_id": cfg.run_id,
            "cluster_id": int(cluster_id),
        }
        for i, feature_name in enumerate(FEATURE_COLUMNS):
            row[f"centroid_{feature_name}"] = float(center[i])
        rows.append(row)

    return spark.createDataFrame(rows).withColumn("created_at", F.current_timestamp())


def build_profiles_df(cfg: AppConfig, predictions: DataFrame) -> DataFrame:
    base = predictions.withColumn("cluster_id", F.col("prediction").cast("int"))

    profiles = base.groupBy("cluster_id").agg(
        F.count("*").alias("cluster_size"),
        *[F.avg(c).alias(f"avg_{c}") for c in FEATURE_COLUMNS],
    )

    genre_counts = (
        base.withColumn("main_genre", F.coalesce(F.col("main_genre"), F.lit("unknown")))
        .groupBy("cluster_id", "main_genre")
        .agg(F.count("*").alias("genre_count"))
    )

    cluster_window = Window.partitionBy("cluster_id")
    rank_window = Window.partitionBy("cluster_id").orderBy(F.col("genre_count").desc(), F.col("main_genre"))

    top_genres = (
        genre_counts.withColumn("cluster_total", F.sum("genre_count").over(cluster_window))
        .withColumn("genre_ratio", F.col("genre_count") / F.col("cluster_total"))
        .withColumn("genre_rank", F.row_number().over(rank_window))
        .filter(F.col("genre_rank") <= 5)
        .groupBy("cluster_id")
        .agg(
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("main_genre").alias("genre"),
                        F.col("genre_count").alias("genre_count"),
                        F.round(F.col("genre_ratio"), 6).alias("genre_ratio"),
                    )
                )
            ).alias("top_genres_json")
        )
    )

    return (
        profiles.join(top_genres, on="cluster_id", how="left")
        .withColumn("run_id", F.lit(cfg.run_id))
        .withColumn("created_at", F.current_timestamp())
    )


def build_assignments_df(cfg: AppConfig, predictions: DataFrame) -> DataFrame:
    genres_json = F.when(F.col("genres").isNull(), F.lit("[]")).otherwise(F.to_json(F.col("genres")))

    return predictions.select(
        F.lit(cfg.run_id).alias("run_id"),
        F.col("song_id"),
        F.coalesce(F.col("main_genre"), F.lit("unknown")).alias("main_genre"),
        genres_json.alias("genres_json"),
        F.col("prediction").cast("int").alias("cluster_id"),
        *[F.col(name).cast("double").alias(name) for name in FEATURE_COLUMNS],
        F.current_timestamp().alias("assigned_at"),
    )


def build_genre_signal_assignments_for_kafka(cfg: AppConfig, predictions: DataFrame) -> DataFrame:
    base = predictions.withColumn("cluster_id", F.col("prediction").cast("int"))
    empty_genres = F.from_json(F.lit("[]"), T.ArrayType(T.StringType()))

    payload = F.struct(
        F.lit("song_cluster_assignment").alias("signal_type"),
        F.lit(1).alias("schema_version"),
        F.lit(cfg.run_id).alias("run_id"),
        F.current_timestamp().cast("string").alias("produced_at_utc"),
        F.lit(cfg.kafka_input_topic).alias("source_topic"),
        F.col("song_id"),
        F.coalesce(F.col("main_genre"), F.lit("unknown")).alias("main_genre"),
        F.coalesce(F.col("genres"), empty_genres).alias("genres"),
        F.col("cluster_id"),
        F.struct(*[F.col(name).alias(name) for name in FEATURE_COLUMNS]).alias("features"),
        F.lit("spark_kmeans").alias("model_name"),
        F.lit("v1").alias("model_version"),
    )

    return base.select(
        F.col("song_id").alias("key"),
        F.to_json(payload).alias("value"),
    )


def build_genre_signal_profiles_for_kafka(
    cfg: AppConfig,
    profiles_df: DataFrame,
    centroids_df: DataFrame,
) -> DataFrame:
    joined = profiles_df.join(
        centroids_df.drop("created_at"),
        on=["run_id", "cluster_id"],
        how="left",
    )

    payload = F.struct(
        F.lit("cluster_profile").alias("signal_type"),
        F.lit(1).alias("schema_version"),
        F.col("run_id"),
        F.current_timestamp().cast("string").alias("produced_at_utc"),
        F.lit(cfg.kafka_input_topic).alias("source_topic"),
        F.col("cluster_id"),
        F.col("cluster_size"),
        F.col("top_genres_json"),
        F.struct(*[F.col(f"centroid_{name}").alias(name) for name in FEATURE_COLUMNS]).alias("centroid_features"),
        F.lit("spark_kmeans").alias("model_name"),
        F.lit("v1").alias("model_version"),
    )

    key_col = F.concat(F.col("run_id"), F.lit(":"), F.col("cluster_id").cast("string"))
    return joined.select(
        key_col.alias("key"),
        F.to_json(payload).alias("value"),
    )


def main() -> None:
    cfg = load_config()
    spark = build_spark_session(cfg)
    started_at = datetime.now(timezone.utc)

    print(f"[INFO] Run ID: {cfg.run_id}")
    print(f"[INFO] Kafka source: {cfg.kafka_bootstrap_servers} / topic={cfg.kafka_input_topic}")
    print(f"[INFO] Kafka output topic: {cfg.kafka_output_topic}")

    raw_count, deduped_df = read_song_metadata(spark, cfg)
    dedup_count = deduped_df.count()

    if dedup_count == 0:
        raise RuntimeError("No valid records were read from Kafka topic song-metadata.")

    assembled_full = ensure_features_vector(deduped_df).cache()
    full_training_rows = assembled_full.count()

    selection_df = assembled_full
    selection_rows = full_training_rows
    if 0 < cfg.k_selection_sample_fraction < 1.0:
        selection_df = assembled_full.sample(
            withReplacement=False,
            fraction=cfg.k_selection_sample_fraction,
            seed=cfg.kmeans_seed,
        ).cache()
        selection_rows = selection_df.count()

    print(f"[INFO] Records parsed from Kafka: {raw_count}")
    print(f"[INFO] Unique songs for clustering: {dedup_count}")
    print(f"[INFO] Rows used for K selection: {selection_rows}")
    if cfg.input_max_rows is not None and cfg.input_max_rows > 0:
        print(f"[INFO] Input max rows applied: {cfg.input_max_rows}")

    metrics_rows: list[dict[str, Any]] = []
    candidate_results: dict[int, dict[str, Any]] = {}

    for k in cfg.candidate_ks:
        _, _, silhouette, training_cost, num_clusters_found = fit_kmeans(selection_df, k, cfg)
        candidate_results[k] = {
            "silhouette": silhouette,
            "training_cost": training_cost,
            "num_clusters_found": num_clusters_found,
        }
        metrics_rows.append(
            {
                "run_id": cfg.run_id,
                "evaluation_scope": "candidate",
                "k": int(k),
                "silhouette": silhouette,
                "training_cost": training_cost,
                "num_clusters_found": int(num_clusters_found),
                "evaluated_on_rows": int(selection_rows),
                "is_recommended": False,
                "is_final": False,
            }
        )

    recommended_k = max(
        candidate_results.keys(),
        key=lambda k: (
            candidate_results[k]["silhouette"],
            -1 * (candidate_results[k]["training_cost"] or 0.0),
        ),
    )

    final_k = cfg.final_k if cfg.final_k is not None else recommended_k

    for row in metrics_rows:
        if row["k"] == recommended_k:
            row["is_recommended"] = True

    print(f"[INFO] Recommended K: {recommended_k}")
    print(f"[INFO] Final K used: {final_k}")

    final_model, final_predictions, final_silhouette, final_training_cost, final_cluster_count = fit_kmeans(
        assembled_full,
        final_k,
        cfg,
    )
    final_predictions = final_predictions.cache()

    metrics_rows.append(
        {
            "run_id": cfg.run_id,
            "evaluation_scope": "final",
            "k": int(final_k),
            "silhouette": float(final_silhouette),
            "training_cost": final_training_cost,
            "num_clusters_found": int(final_cluster_count),
            "evaluated_on_rows": int(full_training_rows),
            "is_recommended": bool(final_k == recommended_k),
            "is_final": True,
        }
    )

    song_clusters_df = build_assignments_df(cfg, final_predictions)
    centroids_df = build_centroids_df(spark, cfg, final_model)
    profiles_df = build_profiles_df(cfg, final_predictions)

    metrics_df = spark.createDataFrame(metrics_rows).withColumn("evaluated_at", F.current_timestamp())

    finished_at = datetime.now(timezone.utc)
    batch_run_row = [
        {
            "run_id": cfg.run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "kafka_topic": cfg.kafka_input_topic,
            "input_rows_raw": int(raw_count),
            "input_rows_dedup": int(dedup_count),
            "candidate_ks": ",".join(str(k) for k in cfg.candidate_ks),
            "recommended_k": int(recommended_k),
            "final_k": int(final_k),
            "final_silhouette": float(final_silhouette),
            "final_training_cost": final_training_cost,
            "notes": (
                f"selection_sample_fraction={cfg.k_selection_sample_fraction}; "
                f"selection_rows={selection_rows}; full_rows={full_training_rows}"
            ),
        }
    ]
    batch_runs_df = spark.createDataFrame(batch_run_row).withColumn("created_at", F.current_timestamp())

    write_to_postgres(batch_runs_df, cfg, cfg.table_batch_runs)
    write_to_postgres(metrics_df, cfg, cfg.table_cluster_metrics)
    write_to_postgres(song_clusters_df, cfg, cfg.table_song_clusters)
    write_to_postgres(centroids_df, cfg, cfg.table_cluster_centroids)
    write_to_postgres(profiles_df, cfg, cfg.table_cluster_profiles)

    assignment_signals_df = build_genre_signal_assignments_for_kafka(cfg, final_predictions)
    profile_signals_df = build_genre_signal_profiles_for_kafka(cfg, profiles_df, centroids_df)
    write_to_kafka_messages(assignment_signals_df, cfg)
    write_to_kafka_messages(profile_signals_df, cfg)

    print("[INFO] Batch KMeans job completed successfully.")
    print(f"[INFO] PostgreSQL target schema: {cfg.postgres_schema}")
    print(f"[INFO] Kafka output topic published: {cfg.kafka_output_topic}")
    print(f"[INFO] run_id={cfg.run_id}")

    spark.stop()


if __name__ == "__main__":
    main()
