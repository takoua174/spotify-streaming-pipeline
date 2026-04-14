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
  → mood-index    (score humeur basé sur valence+energy réelles)
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
TOPIC_MOOD_INDEX  = os.getenv("TOPIC_MOOD_INDEX",   "mood-index")
DATA_DIR          = os.getenv("DATA_DIR",    "./data")
TRACKS_CSV        = os.getenv("TRACKS_CSV",  "songs.csv")
MXM_TRAIN         = os.getenv("MXM_TRAIN",   "mxm_dataset_train.txt")
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

def load_song_catalog(csv_path: str, mxm_path: str, n: int = CATALOG_SIZE) -> list[dict]:
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

    # ── 2. Charge Musixmatch (bag-of-words) ──────────────────
    mxm_index = load_mxm_top_words(mxm_path)
    log.info(f"→ {len(mxm_index)} chansons avec données Musixmatch")

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
            # Top 5 mots des paroles si disponible dans Musixmatch
            "top_words":    mxm_index.get(song_id, [])
        }
        catalog.append(entry)

    log.info(f"✓ Catalogue chargé : {len(catalog)} chansons prêtes pour la simulation")
    return catalog

# top_n: number of words to extract per song
def load_mxm_top_words(mxm_path: str, top_n: int = 5) -> dict:
    """
    Parse le fichier Musixmatch bag-of-words et extrait les top N mots
    les plus fréquents pour chaque chanson.

    Format du fichier :
      - Lignes commençant par # ou % → ignorées (commentaires / vocabulaire)

    Le vocabulaire (ligne %) est parsé pour mapper idx → mot.
    Retourne : { track_id: ["word1", "word2", ...] }
    exemple :
    {
        "track_1": ["love", "baby", "night"],
        "track_2": ["fire", "dance", "party"]
    }
    remark about the file : 
    Each song line looks like: TR123,456,12:3,45:10,78:1
    Meaning:
    Part	Meaning
    TR123	track ID
    456	    internal ID
    12:3	word index 12 appears 3 times
    45:10	word index 45 appears 10 times
    """
    if not os.path.exists(mxm_path):
        log.warning(f"Musixmatch non trouvé : {mxm_path} — simulation sans paroles")
        return {}

    log.info(f"Parsing Musixmatch : {mxm_path} ...")
    vocabulary = []   # liste des mots : 1- based 
    mxm_data   = {}   # { track_id: {word_idx: count} }

    with open(mxm_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            # Commentaire → skip
            if line.startswith("#"):
                continue

            # Ligne vocabulaire (commence par %)
            if line.startswith("%"):
                vocabulary = line[1:].split(",")
                log.info(f"→ Vocabulaire Musixmatch : {len(vocabulary)} mots")
                continue

            # Ligne de données : TRACK_ID,MXM_ID,idx:count,...
            parts = line.split(",")
            if len(parts) < 3:
                continue

            track_id = parts[0]
            word_counts = {}
            for token in parts[2:]:
                try:
                    idx, count = token.split(":")
                    word_counts[int(idx)] = int(count)
                except ValueError:
                    continue

            # Garde les top_n mots les plus fréquents
            if word_counts and vocabulary:
                top = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
                mxm_data[track_id] = [
                    vocabulary[idx - 1]          # idx est 1-based 
                    for idx, _ in top
                    if 1 <= idx <= len(vocabulary)
                ]

    log.info(f"→ {len(mxm_data)} chansons parsées depuis Musixmatch")
    return mxm_data


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
        "top_words":             song["top_words"],
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


def compute_mood_score(song: dict) -> dict:
    """
    Score d'humeur basé sur valence + energy RÉELLES de la chanson.

    Quadrants :
      valence↑ energy↑ → happy
      valence↑ energy↓ → relaxed
      valence↓ energy↑ → energetic/tense
      valence↓ energy↓ → melancholic
    """
    v = song["valence"]
    e = song["energy"]
    mood_score = round(v * 0.6 + e * 0.4, 4)

    if v >= 0.6 and e >= 0.6:
        label = "happy"
    elif v >= 0.6 and e < 0.6:
        label = "relaxed"
    elif v < 0.6 and e >= 0.6:
        label = "energetic"
    else:
        label = "melancholic"

    return {
        "event_id":   str(uuid.uuid4()),
        "song_id":    song["song_id"],
        "valence":    v,
        "energy":     e,
        "mood_score": mood_score,
        "mood_label": label,
        "top_words":  song["top_words"],   # paroles réelles → utile pour NLP Couche 3a
        "timestamp":  datetime.utcnow().isoformat()
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
        mxm_path=os.path.join(DATA_DIR, MXM_TRAIN)
    )

    log.info(f"Démarrage simulation → {len(catalog)} chansons, {NUM_USERS} utilisateurs fictifs")
    log.info(f"Topics : {TOPIC_SONG_PLAYS}, {TOPIC_USER_EVENTS}, {TOPIC_MOOD_INDEX}")

    producer = create_producer()
    count    = 0

    try:
        while max_events is None or count < max_events:
            user_id  = f"user_{random.randint(1, NUM_USERS):05d}"
            song     = random.choice(catalog)   # vraie chanson
            evt_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

            # ── play → 3 topics ──────────────────────────────────
            if evt_type == "play":
                producer.send(TOPIC_SONG_PLAYS,
                              value=generate_play_event(user_id, song)) \
                        .add_errback(on_send_error)

                producer.send(TOPIC_MOOD_INDEX,
                              value=compute_mood_score(song)) \
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