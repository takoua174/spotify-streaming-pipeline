"""
producer_batch_csv.py
=====================
SOURCE : Dataset Kaggle (songs.csv + artists.csv)

Colonnes confirmées songs.csv (551K lignes) :
  id, name, album_name, artists, danceability, energy, key, loudness,
  mode, speechiness, acousticness, instrumentalness, liveness,
  valence, tempo, duration_ms, explicit

Colonnes confirmées artists.csv (71K lignes) :
  id, name, followers, popularity, genres, main_genre

Envoie vers :
  → song-metadata   (features audio complètes + album + artiste)
  → genre-signals   (main_genre + genres tags depuis artists.csv)
"""

import json
import os
import time
import logging
import ast
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer_batch_csv")

BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SONG_METADATA = os.getenv("TOPIC_SONG_METADATA",  "song-metadata")
TOPIC_GENRE_SIGNALS = os.getenv("TOPIC_GENRE_SIGNALS",  "genre-signals")
DATA_DIR            = os.getenv("DATA_DIR",    "./data")
TRACKS_CSV          = os.getenv("TRACKS_CSV",  "songs.csv")
ARTISTS_CSV         = os.getenv("ARTISTS_CSV", "artists.csv")

BATCH_SIZE      = 500
BATCH_PAUSE_SEC = 0.1


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=10
    )


def load_songs(filepath: str) -> pd.DataFrame:
    """
    Charge songs.csv avec les colonnes confirmées du dataset Kaggle.
    """
    log.info(f"Chargement de {filepath} ...")
    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"→ {len(df)} chansons chargées, colonnes : {list(df.columns)}")

    # Colonnes confirmées
    expected = [
        "id", "name", "album_name", "artists",
        "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "duration_ms", "explicit"
    ]
    available = [c for c in expected if c in df.columns]
    missing   = [c for c in expected if c not in df.columns]
    if missing:
        log.warning(f"Colonnes absentes dans songs.csv : {missing}")

    df = df[available].fillna({
        "name": "Unknown", "album_name": "Unknown", "artists": "[]",
        "danceability": 0.0, "energy": 0.0, "key": 0, "loudness": -60.0,
        "mode": 0, "speechiness": 0.0, "acousticness": 0.0,
        "instrumentalness": 0.0, "liveness": 0.0, "valence": 0.0,
        "tempo": 0.0, "duration_ms": 0, "explicit": False
    })
    return df


def load_artists(filepath: str) -> dict:
    """
    Charge artists.csv et construit un dict { artist_name → row }
    pour la jointure avec songs.csv.
    Le champ 'artists' dans songs.csv est une liste Python sérialisée
    ex: "['Radiohead']" → on extrait le premier artiste pour la jointure.
    """
    if not os.path.exists(filepath):
        log.warning(f"artists.csv non trouvé à {filepath}, enrichissement désactivé.")
        return {}

    log.info(f"Chargement de {filepath} ...")
    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"→ {len(df)} artistes chargés")

    df = df.fillna({
        "name": "Unknown", "followers": 0,
        "popularity": 0, "genres": "[]", "main_genre": "Unknown"
    })

    # Index par nom d'artiste (minuscule pour la jointure)
    return {
        str(row["name"]).lower(): row.to_dict()
        for _, row in df.iterrows()
    }


def parse_artists_field(raw: str) -> list:
    """
    songs.csv stocke les artistes comme une liste Python sérialisée :
      "['Radiohead']"  ou  "['Jay-Z', 'Alicia Keys']"
    Cette fonction la convertit en vraie liste Python.
    """
    try:
        return ast.literal_eval(raw)
    except Exception:
        return [str(raw)]


def row_to_metadata_event(row: dict, artist_info: dict) -> dict:
    """
    Construit le message song-metadata.
    Fusionne les infos de songs.csv et artists.csv.
    """
    artists_list = parse_artists_field(row.get("artists", "[]"))
    primary_artist = artists_list[0] if artists_list else "Unknown"

    # Cherche l'artiste principal dans le dict artists
    artist_data = artist_info.get(primary_artist.lower(), {})

    return {
        "song_id":           str(row.get("id", "")),
        "title":             str(row.get("name", "Unknown")),
        "album_name":        str(row.get("album_name", "Unknown")),
        "artists":           artists_list,
        "primary_artist":    primary_artist,
        # Features audio (toutes confirmées dans songs.csv)
        "danceability":      float(row.get("danceability", 0)),
        "energy":            float(row.get("energy", 0)),
        "key":               int(row.get("key", 0)),
        "loudness":          float(row.get("loudness", -60)),
        "mode":              int(row.get("mode", 0)),
        "speechiness":       float(row.get("speechiness", 0)),
        "acousticness":      float(row.get("acousticness", 0)),
        "instrumentalness":  float(row.get("instrumentalness", 0)),
        "liveness":          float(row.get("liveness", 0)),
        "valence":           float(row.get("valence", 0)),
        "tempo":             float(row.get("tempo", 0)),
        "duration_ms":       int(row.get("duration_ms", 0)),
        "explicit":          bool(row.get("explicit", False)),
        # Infos artiste enrichies depuis artists.csv
        "artist_followers":  int(artist_data.get("followers", 0)),
        "artist_popularity": int(artist_data.get("popularity", 0)),
        "source":            "kaggle_songs_csv"
    }


def row_to_genre_signal(row: dict, artist_info: dict) -> dict:
    """
    Construit le message genre-signals.
    Utilise main_genre et genres depuis artists.csv.
    Le vecteur features servira au K-Means en Couche 3a.
    """
    artists_list   = parse_artists_field(row.get("artists", "[]"))
    primary_artist = artists_list[0] if artists_list else "Unknown"
    artist_data    = artist_info.get(primary_artist.lower(), {})

    # Genres depuis artists.csv
    raw_genres = artist_data.get("genres", "[]")
    try:
        genres_list = ast.literal_eval(raw_genres) if isinstance(raw_genres, str) else []
    except Exception:
        genres_list = []

    return {
        "song_id":    str(row.get("id", "")),
        "main_genre": str(artist_data.get("main_genre", "Unknown")),
        "genres":     genres_list,
        # Vecteur audio pour K-Means clustering (Couche 3a)
        "features": {
            "danceability":     float(row.get("danceability", 0)),
            "energy":           float(row.get("energy", 0)),
            "acousticness":     float(row.get("acousticness", 0)),
            "instrumentalness": float(row.get("instrumentalness", 0)),
            "valence":          float(row.get("valence", 0)),
            "speechiness":      float(row.get("speechiness", 0)),
            "liveness":         float(row.get("liveness", 0)),
            "loudness_norm":    max(0.0, (float(row.get("loudness", -60)) + 60) / 60),
            # tempo normalisé entre 0 et 1 (max 250 BPM)
            "tempo_norm":       min(float(row.get("tempo", 0)) / 250.0, 1.0),
        },
        "source": "kaggle_batch"
    }


def run():
    songs_path   = os.path.join(DATA_DIR, TRACKS_CSV)
    artists_path = os.path.join(DATA_DIR, ARTISTS_CSV)

    if not os.path.exists(songs_path):
        log.error(f"Fichier non trouvé : {songs_path}")
        log.error("Place songs.csv dans ./data/ depuis le dataset Kaggle.")
        return

    log.info(f"Démarrage producer batch CSV → broker={BOOTSTRAP_SERVERS}")

    producer     = create_producer()
    artist_index = load_artists(artists_path)   # dict name → row
    df           = load_songs(songs_path)
    total        = len(df)
    sent         = 0

    try:
        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # ── Topic 1 : song-metadata ──────────────────────────
            meta = row_to_metadata_event(row_dict, artist_index)
            producer.send(
                TOPIC_SONG_METADATA,
                value=meta,
                key=meta["song_id"].encode("utf-8")
            )

            # ── Topic 2 : genre-signals ──────────────────────────
            genre = row_to_genre_signal(row_dict, artist_index)
            producer.send(
                TOPIC_GENRE_SIGNALS,
                value=genre,
                key=genre["song_id"].encode("utf-8")
            )

            sent += 1
            if sent % BATCH_SIZE == 0:
                producer.flush()
                pct = sent * 100 // total
                log.info(f"→ {sent}/{total} chansons envoyées ({pct}%)")
                time.sleep(BATCH_PAUSE_SEC)

        producer.flush()
        log.info(f"✓ Batch terminé : {sent} chansons envoyées dans Kafka")

    except KeyboardInterrupt:
        log.info("Batch interrompu par l'utilisateur.")
    except Exception as e:
        log.error(f"Erreur : {e}")
        raise
    finally:
        producer.close()
        log.info("Producer batch fermé.")


if __name__ == "__main__":
    run()