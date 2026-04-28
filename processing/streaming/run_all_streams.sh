#!/bin/bash
# run_all_streams.sh

echo "Prefetching dependencies..."

/opt/spark/bin/spark-submit \
  --packages $SPARK_PACKAGES \
  --master local[1] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 2 > /dev/null

echo "Starting Streaming Jobs..."

SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

cd /opt/pipeline/processing/streaming

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=768m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  mood_tracker.py &
MOOD_PID=$!

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=768m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  trend_detector.py &
TREND_PID=$!

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=768m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  anomaly_detector.py &
ANOMALY_PID=$!

echo "All jobs submitted. View jobs in Spark UI (http://localhost:8080)."
wait $MOOD_PID $TREND_PID $ANOMALY_PID
