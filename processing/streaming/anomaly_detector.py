from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, countDistinct, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("AnomalyDetector") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    user_event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("session_id", StringType(), True)
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.select(from_json(col("value").cast("string"), user_event_schema).alias("data")).select("data.*")

    # Anomaly 1: > 100 plays of same song by a user in 1 minute (Bot suspicion)
    bot_suspicion_df = parsed_df \
        .filter(col("event_type") == "play") \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("user_id"), col("song_id")) \
        .agg(count("event_id").alias("play_count")) \
        .filter(col("play_count") > 100) \
        .withColumn("anomaly_type", col("user_id").cast("string")) # Hack for placeholder
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_id"),
            col("anomaly_type").alias("anomaly_type"), # Replaced later
            col("play_count").cast("string").alias("description")
        )
    
    bot_suspicion_df = bot_suspicion_df \
        .withColumn("anomaly_type", when(col("play_count") > -1, "bot_suspicion")) \
        .withColumn("description", when(col("play_count") > -1, "More than 100 plays in 1 min"))
    
    def write_to_cassandra(batch_df, batch_id):
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spotify_streaming") \
            .option("table", "anomalies") \
            .mode("append") \
            .save()

    out_query_bots = bot_suspicion_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/checkpoints_anomalies_bots") \
        .start()

    out_query_bots.awaitTermination()

if __name__ == "__main__":
    main()
