#!/bin/bash

# Define dependencies
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"

echo "======================================"
echo "Starting Trend Detector Stream..."
echo "Press Ctrl+C to stop this job."
echo "======================================"

cd /opt/pipeline/processing/streaming

/opt/spark/bin/spark-submit --packages $SPARK_PACKAGES \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=768m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  trend_detector.py