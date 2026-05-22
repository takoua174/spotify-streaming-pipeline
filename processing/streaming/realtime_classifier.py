from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import math
from datetime import datetime

def main():
    spark = SparkSession.builder \
        .appName("RealtimeSongClassifier") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.localDC", "datacenter1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Fetch latest centroids from genre-signal
    print("Fetching latest centroids from 'genre-signal' topic...")
    try:
        centroids_raw_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "genre-signals") \
            .option("startingOffsets", "earliest") \
            .load()
            
        signal_schema = StructType([
            StructField("signal_type", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("cluster_id", IntegerType(), True),
            StructField("centroid_features", StructType([
                StructField("danceability", FloatType(), True),
                StructField("energy", FloatType(), True),
                StructField("acousticness", FloatType(), True),
                StructField("instrumentalness", FloatType(), True),
                StructField("valence", FloatType(), True),
                StructField("speechiness", FloatType(), True),
                StructField("liveness", FloatType(), True),
                StructField("loudness_norm", FloatType(), True),
                StructField("tempo_norm", FloatType(), True)
            ]), True)
        ])

        parsed_centroids = centroids_raw_df.select(
            from_json(col("value").cast("string"), signal_schema).alias("data")
        ).select("data.*")
        
        # Get the latest run_id by finding the last row's run_id or taking top 10
        # Since Kafka preserves order, we can collect all and find the latest run_id
        all_centers = parsed_centroids.collect()
        
        if len(all_centers) == 0:
            print("No centroids found in topic. Falling back to default or waiting.")
            return

        latest_run_id = all_centers[-1]["run_id"]
        latest_centroids = [row.asDict(recursive=True) for row in all_centers if row["run_id"] == latest_run_id]
        
        # We need exactly 10 (or whatever number of clusters was generated)
        # We'll broadcast this dictionary to all executors
        centroids_list = latest_centroids
        print(f"Loaded {len(centroids_list)} centroids from run: {latest_run_id}")
        
    except Exception as e:
        print(f"Failed to fetch centroids from kafka: {e}")
        return

    # Broadcast centroids so executors can compute distance locally
    broadcast_centroids = spark.sparkContext.broadcast(centroids_list)

    # 2. Consume from song-to-classify
    song_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("timestamp", StringType(), True), # ISO format
        StructField("features", StructType([
            StructField("danceability", FloatType(), True),
            StructField("energy", FloatType(), True),
            StructField("acousticness", FloatType(), True),
            StructField("instrumentalness", FloatType(), True),
            StructField("valence", FloatType(), True),
            StructField("speechiness", FloatType(), True),
            StructField("liveness", FloatType(), True),
            StructField("loudness", FloatType(), True),
            StructField("tempo", FloatType(), True)
        ]), True)
    ])

    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "song-to-classify") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_stream = stream_df.select(
        from_json(col("value").cast("string"), song_schema).alias("data")
    ).select("data.*")

    # Define UDF to find closest cluster
    def find_closest_cluster(features_row):
        if not features_row:
            return -1
            
        features_dict = features_row.asDict()
        centers = broadcast_centroids.value
        min_dist = float('inf')
        best_cluster = -1
        
        # Min-max normalization values from original kmeans (approximate bounds if not strictly [0,1])
        # In actual prod we should broadcast the fitted scaler, but since we simply want to classify:
        # For loudness typically [-60, 0] so norm = (l - -60)/60. Tempo typically [0, 250] so norm = t/250.
        f_loudness = (features_dict.get('loudness', 0) - (-60.0)) / 60.0
        f_tempo = features_dict.get('tempo', 0) / 250.0
        
        # Clip max/min
        f_loudness = max(0.0, min(1.0, f_loudness))
        f_tempo = max(0.0, min(1.0, f_tempo))

        for center in centers:
            cf = center['centroid_features']
            c_id = center['cluster_id']
            
            # Euclidean distance
            dist = math.sqrt(
                (features_dict.get('danceability', 0) - cf.get('danceability', 0))**2 +
                (features_dict.get('energy', 0) - cf.get('energy', 0))**2 +
                (features_dict.get('acousticness', 0) - cf.get('acousticness', 0))**2 +
                (features_dict.get('instrumentalness', 0) - cf.get('instrumentalness', 0))**2 +
                (features_dict.get('valence', 0) - cf.get('valence', 0))**2 +
                (features_dict.get('speechiness', 0) - cf.get('speechiness', 0))**2 +
                (features_dict.get('liveness', 0) - cf.get('liveness', 0))**2 +
                (f_loudness - cf.get('loudness_norm', 0))**2 +
                (f_tempo - cf.get('tempo_norm', 0))**2
            )
            
            if dist < min_dist:
                min_dist = dist
                best_cluster = c_id
                
        return best_cluster

    find_cluster_udf = udf(find_closest_cluster, IntegerType())

    # Apply UDF
    classified_df = parsed_stream \
        .withColumn("cluster_id", find_cluster_udf(col("features"))) \
        .withColumn("classified_at", lit(datetime.now())) \
        .select(
            col("song_id"),
            col("title"),
            col("artist"),
            col("cluster_id"),
            col("classified_at")
        )

    def write_to_cassandra(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spotify_streaming") \
            .option("table", "song_classifications") \
            .mode("append") \
            .save()

    print("Starting streaming query...")
    query = classified_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/checkpoints_song_classifier") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()