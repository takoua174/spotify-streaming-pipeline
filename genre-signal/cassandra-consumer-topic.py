
import json
from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster

cassandra_cluster = Cluster(["localhost"])
session = cassandra_cluster.connect("spotify_streaming")

print("Truncating cluster_centroids table...")
session.execute("TRUNCATE cluster_centroids")
print("Table cleared.")

insert_stmt = session.prepare("""
    INSERT INTO cluster_centroids (
        cluster_id, run_id, cluster_size, top_genres,
        danceability, energy, acousticness, instrumentalness,
        valence, speechiness, liveness, loudness_norm, tempo_norm
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "centroid-loader-v2",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["genre-signals"])
print("Reading from genre-signals topic...")

count = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of topic reached.")
                break
            else:
                print(f"Kafka error: {msg.error()}")
            continue

        raw = json.loads(msg.value().decode("utf-8"))

        # Skip messages that are not cluster profiles
        if raw.get("signal_type") != "cluster_profile":
            print(f"Skipping message with signal_type={raw.get('signal_type')}")
            continue

        # Extract nested fields
        features   = raw.get("centroid_features", {})
        top_genres = raw.get("top_genres_json", "[]")

        # top_genres_json is already a JSON string → store as-is (text column)
        # parse it just to extract a readable summary if you want, else store raw
        genres_parsed = json.loads(top_genres)
        top_genres_summary = ", ".join(
            f"{g['genre']}({round(g['genre_ratio']*100)}%)" for g in genres_parsed
        )

        cluster_id = raw.get("cluster_id")
        print(f"Writing cluster_id={cluster_id} | size={raw.get('cluster_size')} | top={top_genres_summary}")

        session.execute(insert_stmt, (
            cluster_id,
            raw.get("run_id"),
            raw.get("cluster_size"),
            top_genres_summary,               # stored as readable text
            float(features.get("danceability", 0)),
            float(features.get("energy", 0)),
            float(features.get("acousticness", 0)),
            float(features.get("instrumentalness", 0)),
            float(features.get("valence", 0)),
            float(features.get("speechiness", 0)),
            float(features.get("liveness", 0)),
            float(features.get("loudness_norm", 0)),
            float(features.get("tempo_norm", 0)),
        ))
        count += 1
        print(f"  cluster_id={cluster_id} inserted OK")

except KeyboardInterrupt:
    print("Stopped.")
finally:
    consumer.close()
    cassandra_cluster.shutdown()
    print(f"Done. {count} centroids written.")
