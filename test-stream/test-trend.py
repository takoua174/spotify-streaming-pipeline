import json
import uuid
import os
import random
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SONG_PLAYS = os.getenv("TOPIC_SONG_PLAYS", "song-plays")

def test_trend():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Injecting 15 rapid events for a single song into '{TOPIC_SONG_PLAYS}'...")

    for i in range(15):
        # We vary the user_id so it looks like different people are playing it
        random_user = f"user_{random.randint(1000, 9999)}"
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "play",
            "user_id": random_user,
            "song_id": "sahbi-viral_song_999",
            "title": "Super Viral Hit",
            "artist": "The Testers",
            "energy": 0.8,
            "valence": 0.9,
            "tempo": 125.0,
            "danceability": 0.85,
            "duration_listened_sec": 120,
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send(TOPIC_SONG_PLAYS, value=event)
        print(f"Sent trend event {i+1}/15")

    producer.flush()
    producer.close()
    print("Done! Check your 'trending_songs' table in Cassandra.")

if __name__ == "__main__":
    test_trend()