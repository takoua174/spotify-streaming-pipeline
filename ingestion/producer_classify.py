"""
producer_classify.py
======================
Reads songs.csv and pushes 1 random song per second
to the 'song-to-classify' Kafka topic.
"""

import json
import time
import os
import logging
import ast
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer_classify")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TO_CLASSIFY = "song-to-classify"
DATA_DIR          = os.getenv("DATA_DIR", "./data")
TRACKS_CSV        = os.getenv("TRACKS_CSV", "songs.csv")
INTERVAL_MS       = 1000  # 1 song per second

def load_catalog_for_classification(csv_path: str, n: int = 5000) -> list[dict]:
    log.info(f"Loading catalog from {csv_path} ...")
    if not os.path.exists(csv_path):
        log.error(f"Cannot find {csv_path}")
        raise FileNotFoundError(csv_path)

    df = pd.read_csv(csv_path, low_memory=False)
    required = ["id", "name", "artists", "energy", "valence", "tempo", 
                "danceability", "acousticness", "instrumentalness", 
                "speechiness", "liveness", "loudness"]
    
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in songs.csv: {missing}")

    df = df.dropna(subset=required)
    if len(df) > n:
        df = df.sample(n=n, random_state=42)

    catalog = []
    for _, row in df.iterrows():
        try:
            artists_list = ast.literal_eval(str(row.get("artists", "[]")))
        except Exception:
            artists_list = [str(row.get("artists", "Unknown"))]

        msg = {
            "song_id": str(row["id"]),
            "title": str(row["name"]),
            "artist": artists_list[0] if artists_list else "Unknown",
            "features": {
                "danceability": float(row["danceability"]),
                "energy": float(row["energy"]),
                "acousticness": float(row["acousticness"]),
                "instrumentalness": float(row["instrumentalness"]),
                "valence": float(row["valence"]),
                "speechiness": float(row["speechiness"]),
                "liveness": float(row["liveness"]),
                "loudness": float(row["loudness"]),
                "tempo": float(row["tempo"])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        catalog.append(msg)
    
    log.info(f"Loaded {len(catalog)} songs for classification simulation.")
    return catalog

def create_producer() -> KafkaProducer:
    log.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3
    )

def main():
    csv_path = os.path.join(DATA_DIR, TRACKS_CSV)
    catalog = load_catalog_for_classification(csv_path, n=10000)

    producer = create_producer()
    log.info(f"Connected! Starting simulation to topic: {TOPIC_TO_CLASSIFY}")

    try:
        while True:
            song = random.choice(catalog)
            song["timestamp"] = datetime.utcnow().isoformat()
            
            # Send to Kafka
            producer.send(
                topic=TOPIC_TO_CLASSIFY,
                key=song["song_id"],
                value=song
            )
            
            log.info(f"Sent song to classify: {song['title']} by {song['artist']}")
            time.sleep(INTERVAL_MS / 1000.0)
            
    except KeyboardInterrupt:
        log.info("Simulation stopped by user.")
    finally:
        producer.flush()
        producer.close()
        log.info("Kafka producer closed.")

if __name__ == "__main__":
    main()
