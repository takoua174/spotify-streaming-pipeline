$ErrorActionPreference = "Stop"

$PROJECT_ROOT = Resolve-Path "$PSScriptRoot\..\.."
$COMPOSE_FILE = Join-Path $PROJECT_ROOT "docker\docker-compose.yml"
$JOB_FILE = Join-Path $PROJECT_ROOT "processing\batch\kmeans_genre_clustering.py"

Set-Location $PROJECT_ROOT

if (-not (Test-Path $JOB_FILE)) {
  throw "Batch job file not found: $JOB_FILE"
}

docker compose -f $COMPOSE_FILE up -d zookeeper kafka postgres spark-master spark-worker kafka-ui
if ($LASTEXITCODE -ne 0) {
  throw "docker compose up failed with exit code $LASTEXITCODE"
}

$submitCommand = "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4 /opt/pipeline/processing/batch/kmeans_genre_clustering.py"

docker compose -f $COMPOSE_FILE exec -T spark-master /bin/bash -lc $submitCommand
if ($LASTEXITCODE -ne 0) {
  throw "Spark batch job failed with exit code $LASTEXITCODE"
}
