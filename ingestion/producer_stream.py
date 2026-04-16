"""
producer_stream.py
==================
SOURCE : Données réelles Kaggle (songs.csv) + Musixmatch (mxm_dataset_train.txt)

Principe : chansons RÉELLES, événements SIMULÉS.
  - Le catalogue de chansons est chargé depuis songs.csv (Kaggle)
  - Les mots-clés de paroles viennent de mxm_dataset_train.txt (Musixmatch)
  - Génère 3 types d'événements :
        + PLAY  : un utilisateur commence à écouter une chanson
        + SKIP  : un utilisateur passe à la chanson suivante
        + LIKE  : un utilisateur aime une chanson

Envoie vers :
  → song-plays    (événements de lecture sur vraies chansons)
  → user-events   (actions utilisateur avec contexte)
"""

import json
import time
import random
import uuid
import os
import logging
import ast
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer_stream")

# ── Config ───────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SONG_PLAYS  = os.getenv("TOPIC_SONG_PLAYS",   "song-plays")
TOPIC_USER_EVENTS = os.getenv("TOPIC_USER_EVENTS",  "user-events")
DATA_DIR          = os.getenv("DATA_DIR",    "./data")
TRACKS_CSV        = os.getenv("TRACKS_CSV",  "songs.csv")
NUM_USERS         = int(os.getenv("NUM_SIMULATED_USERS", 1000))
INTERVAL_MS       = int(os.getenv("SIMULATION_INTERVAL_MS", 500))

# Nombre de chansons à charger en mémoire pour le simulateur
# (pas besoin des 551K, 10K suffisent pour la simulation)
CATALOG_SIZE = 10_000

EVENT_TYPES   = ["play", "skip", "like"]
EVENT_WEIGHTS = [0.70, 0.20, 0.10]


# ══════════════════════════════════════════════════════════════
# CHARGEMENT DU CATALOGUE RÉEL
# ══════════════════════════════════════════════════════════════

def load_song_catalog(csv_path: str, n: int = CATALOG_SIZE) -> list[dict]:
    """
    Charge N chansons réelles depuis songs.csv et enrichit avec
    les données Musixmatch (top mots des paroles).

    Retourne une liste de dicts prêts à être utilisés par le simulateur.
    """
    log.info(f"Chargement du catalogue depuis {csv_path} ...")

    if not os.path.exists(csv_path):
        log.error(f"songs.csv introuvable : {csv_path}")
        raise FileNotFoundError(csv_path)

    # ── 1. Charge songs.csv ──────────────────────────────────
    df = pd.read_csv(csv_path, low_memory=False) # reads the entire file at once before deciding data types

    required = ["id", "name", "artists", "energy", "valence",
                "tempo", "danceability", "acousticness"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans songs.csv : {missing}")

    # Supprime les lignes sans id ou sans features audio critiques
    df = df.dropna(subset=["id", "energy", "valence"])

    # Échantillon aléatoire pour ne pas charger 551K en RAM
    if len(df) > n:
        df = df.sample(n=n, random_state=42)

    log.info(f"→ {len(df)} chansons sélectionnées dans le catalogue")

    # ── 3. Construit le catalogue final ──────────────────────
    catalog = []
    for _, row in df.iterrows():
        song_id = str(row["id"])

        # Parse le champ artists (liste Python sérialisée)
        try:
            artists_list = ast.literal_eval(str(row.get("artists", "[]")))
        except Exception:
            artists_list = [str(row.get("artists", "Unknown"))]
        primary_artist = artists_list[0] if artists_list else "Unknown"

        entry = {
            "song_id":      song_id,
            "title":        str(row.get("name", "Unknown")),
            "artist":       primary_artist,
            "energy":       float(row.get("energy", 0.5)),
            "valence":      float(row.get("valence", 0.5)),
            "tempo":        float(row.get("tempo", 120)),
            "danceability": float(row.get("danceability", 0.5)),
            "acousticness": float(row.get("acousticness", 0.5)),
        }
        catalog.append(entry)

    log.info(f"✓ Catalogue chargé : {len(catalog)} chansons prêtes pour la simulation")
    return catalog


# ══════════════════════════════════════════════════════════════
# GÉNÉRATEURS D'ÉVÉNEMENTS
# ══════════════════════════════════════════════════════════════

def generate_play_event(user_id: str, song: dict) -> dict:
    """Événement de lecture sur une vraie chanson."""
    return {
        "event_id":              str(uuid.uuid4()),
        "event_type":            "play",
        "user_id":               user_id,
        "song_id":               song["song_id"],
        "title":                 song["title"],
        "artist":                song["artist"],
        # Features audio réelles depuis songs.csv
        "energy":                song["energy"],
        "valence":               song["valence"],
        "tempo":                 song["tempo"],
        "danceability":          song["danceability"],
        # Top mots depuis Musixmatch (vide si chanson non couverte)
        #"top_words":             song["top_words"],
        # Événement simulé
        "duration_listened_sec": random.randint(10, 240),
        "timestamp":             datetime.utcnow().isoformat()
    }


def generate_user_event(user_id: str, song: dict, event_type: str) -> dict:
    """Événement utilisateur enrichi (contexte d'écoute simulé)."""
    return {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "user_id":    user_id,
        "song_id":    song["song_id"],
        "timestamp":  datetime.utcnow().isoformat(),
        # Contexte simulé
        "device":     random.choice(["mobile", "desktop", "tablet"]),
        "country":    random.choice(["TN", "FR", "US", "MA", "DZ", "DE", "GB"]),
        "time_of_day": datetime.utcnow().hour,
        "session_id": str(uuid.uuid4())[:8]
    }


# ══════════════════════════════════════════════════════════════
# PRODUCER
# ══════════════════════════════════════════════════════════════

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"), # Message serializer
        acks="all", #Kafka will only confirm success when: data is written to all replicas + data is safely stored
        retries=3,
        retry_backoff_ms=500
    )


def on_send_error(excp):
    log.error(f"Erreur Kafka : {excp}")


def run_simulation(max_events: int = None):
    """
    Boucle principale :
      1. Charge le catalogue de vraies chansons (une seule fois)
      2. Génère des événements simulés en boucle sur ce catalogue
    """
    # Charge le catalogue réel au démarrage
    catalog = load_song_catalog(
        csv_path=os.path.join(DATA_DIR, TRACKS_CSV),
        #mxm_path=os.path.join(DATA_DIR, MXM_TRAIN)
    )

    log.info(f"Démarrage simulation → {len(catalog)} chansons, {NUM_USERS} utilisateurs fictifs")
    log.info(f"Topics : {TOPIC_SONG_PLAYS}, {TOPIC_USER_EVENTS}")

    producer = create_producer()
    count    = 0

    try:
        while max_events is None or count < max_events:
            user_id  = f"user_{random.randint(1, NUM_USERS):05d}"
            song     = random.choice(catalog)   # vraie chanson
            evt_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

            # ── play → 2 topics ──────────────────────────────────
            if evt_type == "play":
                producer.send(TOPIC_SONG_PLAYS,
                              value=generate_play_event(user_id, song)) \
                        .add_errback(on_send_error)

            # ── tous types → user-events ─────────────────────────
            producer.send(TOPIC_USER_EVENTS,
                          value=generate_user_event(user_id, song, evt_type)) \
                    .add_errback(on_send_error)

            count += 1
            if count % 100 == 0:
                log.info(f"→ {count} événements envoyés")

            time.sleep(INTERVAL_MS / 1000.0)

    except KeyboardInterrupt:
        log.info("Simulation arrêtée.")
    finally:
        producer.flush()
        producer.close()
        log.info(f"Total envoyé : {count} événements.")


if __name__ == "__main__":
    run_simulation()