#!/bin/bash
# run_all_streams.sh

echo "Starting Streaming Jobs..."

SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

cd /opt/pipeline/processing/streaming

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  mood_tracker.py &
MOOD_PID=$!

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  trend_detector.py &
TREND_PID=$!

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  anomaly_detector.py &
ANOMALY_PID=$!

echo "All jobs submitted. View jobs in Spark UI (http://localhost:8080)."
wait $MOOD_PID $TREND_PID $ANOMALY_PID
