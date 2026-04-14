"""
test_consumer.py
================
Outil de TEST pour vérifier que les 5 topics Kafka reçoivent
bien des messages. À lancer pendant que les producers tournent.

Usage :
  python ingestion/test_consumer.py                  # Vérifie tous les topics
  python ingestion/test_consumer.py song-plays       # Vérifie 1 topic spécifique
  python ingestion/test_consumer.py --count 20       # Affiche 20 messages max
"""

import sys
import json
import os
import argparse
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("test_consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ALL_TOPICS = [
    os.getenv("TOPIC_SONG_PLAYS",    "song-plays"),
    os.getenv("TOPIC_USER_EVENTS",   "user-events"),
    os.getenv("TOPIC_SONG_METADATA", "song-metadata"),
    os.getenv("TOPIC_MOOD_INDEX",    "mood-index"),
    os.getenv("TOPIC_GENRE_SIGNALS", "genre-signals"),
]


def consume_topic(topic: str, max_messages: int = 10):
    """
    Consomme et affiche N messages d'un topic Kafka.
    Repart du début (auto_offset_reset='earliest') pour voir
    tous les messages depuis le lancement des producers.
    """
    log.info(f"\n{'='*60}")
    log.info(f"Topic : {topic}")
    log.info(f"{'='*60}")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",           # Repart du premier message : ignored if offsets already committed
        enable_auto_commit=False,               # On ne marque pas les messages comme lus
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,               # Timeout si pas de message pendant 5s
        group_id=f"test-consumer-{topic}"
    )

    count = 0
    try:
        for message in consumer:
            print(f"\n[Partition {message.partition} | Offset {message.offset}]")
            print(json.dumps(message.value, indent=2, ensure_ascii=False))
            count += 1
            if count >= max_messages:
                break
    except Exception as e:
        log.error(f"Erreur lecture topic {topic}: {e}")
    finally:
        consumer.close()

    if count == 0:
        log.warning(f"⚠ Aucun message dans '{topic}'. Producer démarré ?")
    else:
        log.info(f"✓ {count} message(s) lu(s) depuis '{topic}'")

    return count


def main():
    parser = argparse.ArgumentParser(description="Test consumer Kafka")
    parser.add_argument("topic", nargs="?", default=None,
                        help="Topic à lire (défaut : tous les topics)")
    parser.add_argument("--count", type=int, default=5,
                        help="Nombre de messages à afficher (défaut : 5)")
    args = parser.parse_args()

    topics_to_check = [args.topic] if args.topic else ALL_TOPICS

    log.info(f"Connexion à Kafka : {BOOTSTRAP_SERVERS}")
    log.info(f"Topics à vérifier : {topics_to_check}")

    results = {}
    for topic in topics_to_check:
        results[topic] = consume_topic(topic, args.count)

    # Résumé final
    print(f"\n{'='*60}")
    print("RÉSUMÉ DE VÉRIFICATION")
    print(f"{'='*60}")
    for topic, count in results.items():
        status = "✓ OK" if count > 0 else "✗ VIDE"
        print(f"  {status}  {topic:25} ({count} messages lus)")


if __name__ == "__main__":
    main()