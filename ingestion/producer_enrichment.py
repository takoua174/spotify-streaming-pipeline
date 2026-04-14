"""
producer_enrichment.py
======================
SOURCE : APIs externes (boîte violette "APIs externes source de données additionnelle")

Rôle : Simule l'enrichissement de données depuis des APIs externes.
       En production, ici on appellerait MusicBrainz, Last.fm, Genius, etc.
       Pour le dev, on génère des données enrichies fictives.

Envoie vers les topics Kafka :
  → song-metadata  (métadonnées enrichies : paroles, bio artiste...)
  → genre-signals  (tags de genre provenant d'APIs externes)

NOTE : Les vraies intégrations API (Last.fm, Genius) seront ajoutées
       dans les couches suivantes si tu valides cette approche.
"""

import json
import time
import random
import os
import logging
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer_enrichment")

BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SONG_METADATA = os.getenv("TOPIC_SONG_METADATA", "song-metadata")
TOPIC_GENRE_SIGNALS = os.getenv("TOPIC_GENRE_SIGNALS", "genre-signals")

# Tags de genre simulant ceux d'une API musicale
GENRE_TAGS = {
    "pop":        ["catchy", "mainstream", "radio", "melodic"],
    "rock":       ["guitar", "drums", "live", "electric"],
    "hip-hop":    ["rap", "beats", "urban", "bass"],
    "jazz":       ["improvisation", "swing", "acoustic", "complex"],
    "electronic": ["synth", "EDM", "dance", "digital"],
    "classical":  ["orchestral", "piano", "concert", "baroque"]
}


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1   # Mode plus rapide pour l'enrichissement (moins critique)
    )


def simulate_api_enrichment(song_id: str) -> dict:
    """
    Simule ce qu'une API externe retournerait pour une chanson.
    Structure : résumé des paroles + tags + popularité externe.
    """
    genre = random.choice(list(GENRE_TAGS.keys()))
    return {
        "song_id":       song_id,
        "source":        "external_api_enrichment",
        "timestamp":     datetime.utcnow().isoformat(),
        # Simule un résumé de paroles (sera remplacé par Genius API)
        "lyrics_summary": random.choice([
            "Thème : amour perdu et nostalgie",
            "Thème : liberté et voyage",
            "Thème : fête et célébration",
            "Thème : introspection et mélancolie",
            "Thème : engagements sociaux"
        ]),
        # Tags de genre (simule Last.fm)
        "external_genre": genre,
        "genre_tags":     random.sample(GENRE_TAGS[genre], k=2),
        # Popularité externe (simule une API de charts)
        "external_popularity": random.randint(0, 100),
        "play_count_external": random.randint(0, 10_000_000)
    }


def run(num_songs: int = 200, interval_sec: float = 2.0):
    """
    Envoie des données enrichies pour N chansons simulées.
    
    Args:
        num_songs: Nombre de chansons à enrichir
        interval_sec: Délai entre chaque appel API simulé
    """
    log.info(f"Démarrage du producer enrichissement → {num_songs} chansons")

    producer = create_producer()
    try:
        for i in range(num_songs):
            song_id = f"song_{i:05d}"

            enriched = simulate_api_enrichment(song_id)

            # Envoie vers song-metadata (enrichissement)
            producer.send(TOPIC_SONG_METADATA, value=enriched,
                          key=song_id.encode("utf-8"))

            # Extrait et envoie le signal de genre séparément
            genre_signal = {
                "song_id":         song_id,
                "external_genre":  enriched["external_genre"],
                "genre_tags":      enriched["genre_tags"],
                "source":          "external_api",
                "timestamp":       enriched["timestamp"]
            }
            producer.send(TOPIC_GENRE_SIGNALS, value=genre_signal,
                          key=song_id.encode("utf-8"))

            if (i + 1) % 50 == 0:
                log.info(f"→ {i+1}/{num_songs} chansons enrichies")

            time.sleep(interval_sec)

    except KeyboardInterrupt:
        log.info("Enrichissement arrêté.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer enrichissement fermé.")


if __name__ == "__main__":
    run()