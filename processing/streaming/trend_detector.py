from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("TrendDetector") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
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
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), play_schema).alias("data")).select("data.*")

    trends_df = parsed_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("song_id"), col("title"), col("artist")) \
        .count() \
        .withColumnRenamed("count", "play_count") \
        .filter(col("play_count") > 50) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("song_id"),
            col("title"),
            col("artist"),
            col("play_count")
        )

    def write_to_cassandra(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spotify_streaming") \
            .option("table", "trending_songs") \
            .mode("append") \
            .save()

    out_query = trends_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/checkpoints_trends") \
        .start()

    out_query.awaitTermination()

if __name__ == "__main__":
    main()
