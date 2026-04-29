import json
import uuid
import os
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_USER_EVENTS = os.getenv("TOPIC_USER_EVENTS", "user-events")

def test_anomaly():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Injecting 15 rapid events for a single user into '{TOPIC_USER_EVENTS}'...")

    for i in range(15):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "play", # Must be "play" for your Spark filter
            "user_id": "sahbi si l hacker",
            "song_id": "msaken msaken",
            "timestamp": datetime.utcnow().isoformat(),
            "device": "desktop",
            "country": "TN",
            "session_id": "test_session_1"
        }
        producer.send(TOPIC_USER_EVENTS, value=event)
        print(f"Sent anomaly event {i+1}/15")

    producer.flush()
    producer.close()
    print("Done! Check your 'anomalies' table in Cassandra.")

if __name__ == "__main__":
    test_anomaly()