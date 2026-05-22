docker exec -it cassandra cqlsh

SQL
USE spotify_streaming;
2. Check the trending songs

SQL
SELECT * FROM trending_songs LIMIT 5;
3. Check the mood tracker

SQL
SELECT * FROM mood_index LIMIT 5;
4. Check the anomalies

SQL
SELECT * FROM anomalies LIMIT 5;




update 
docker cp storage/cassandra/init.cql cassandra:/init.cql

docker exec -it cassandra cqlsh -f /init.cql

SELECT * FROM cluster_centroids LIMIT 10;

