from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("UserMoodTracker") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.connection.localDC", "datacenter1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "song-plays") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), play_schema).alias("data")).select("data.*")

    user_mood_df = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "2 minutes", "1 minute")) \
        .agg(
            avg("valence").alias("avg_valence"),
            avg("energy").alias("avg_energy")
        ) \
        .withColumn("mood_score", (col("avg_valence") * 0.5) + (col("avg_energy") * 0.5)) \
        .withColumn("bucket", lit("global")) \
        .select(
            col("bucket"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("mood_score"),
            col("avg_valence"),
            col("avg_energy")
        )

    def write_to_cassandra(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spotify_streaming") \
            .option("table", "global_mood_monitor") \
            .mode("append") \
            .save()

    out_query = user_mood_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/checkpoints_user_mood") \
        .start()

    out_query.awaitTermination()

if __name__ == "__main__":
    main()