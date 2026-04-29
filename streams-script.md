
docker cp run_mood.sh spark-master:/opt/pipeline/processing/streaming/
docker cp run_trend.sh spark-master:/opt/pipeline/processing/streaming/
docker cp run_anomaly.sh spark-master:/opt/pipeline/processing/streaming/


docker exec -u root -it spark-master bash


cd /opt/pipeline/processing/streaming

docker exec -it spark-master chmod +x /opt/pipeline/processing/streaming/run_mood.sh /opt/pipeline/processing/streaming/run_trend.sh /opt/pipeline/processing/streaming/run_anomaly.sh