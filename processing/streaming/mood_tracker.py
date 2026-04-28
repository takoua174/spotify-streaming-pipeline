from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("MoodTracker") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.localDC", "datacenter1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schéma de l'événement play (basé sur producer_stream.py)
    play_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("energy", DoubleType(), True),
        StructField("valence", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("duration_listened_sec", IntegerType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # Lire le flux depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "song-plays") \
        .option("startingOffsets", "latest") \
        .load()

    # Convertir JSON en DataFrame
    parsed_df = df.select(from_json(col("value").cast("string"), play_schema).alias("data")).select("data.*")

    # Mettre en place le calcul du mood :
    # Par "artist" comme proxy de "genre" puisque le producer génère sur "artist" et "title" sans "genre" clair.
    mood_df = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("artist")) \
        .agg(
            avg("valence").alias("avg_valence"),
            avg("energy").alias("avg_energy")
        ) \
        .withColumn("mood_score", (col("avg_valence") * 0.5) + (col("avg_energy") * 0.5)) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("artist"),
            col("mood_score"),
            col("avg_valence"),
            col("avg_energy")
        )

    def write_to_both(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spotify_streaming") \
            .option("table", "mood_index") \
            .mode("append") \
            .save()

        batch_df.selectExpr("CAST(window_start AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "mood-index") \
            .save()

    out_query = mood_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_both) \
        .option("checkpointLocation", "/tmp/checkpoints_mood") \
        .start()

    out_query.awaitTermination()

if __name__ == "__main__":
    main()
