"""
producer_enrichment.py
======================
SOURCE : Last.fm API (real data, free key)

Pourquoi Last.fm ?
  - API gratuite, pas de Spotify ID requis
  - Recherche par nom artiste + titre → compatible avec songs.csv
  - Retourne : tags de genre, playcount, listeners, artistes similaires
  - Rate limit généreux : 5 req/sec

Prérequis :
  1. Créer un compte sur https://www.last.fm/api/account/create
  2. Copier la clé API dans .env → LASTFM_API_KEY=xxxxxxxxxxxxxxxx

Envoie vers :
  → song-metadata   

Stratégie de jointure :
  songs.csv  →  nom + artiste  →  Last.fm API  →  tags enrichis
"""

import json
import os
import time
import logging
import ast
import requests
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("producer_enrichment")

# ── Config ────────────────────────────────────────────────────
BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SONG_METADATA = os.getenv("TOPIC_SONG_METADATA",  "song-metadata")
DATA_DIR            = os.getenv("DATA_DIR",   "./data")
TRACKS_CSV          = os.getenv("TRACKS_CSV", "songs.csv")

LASTFM_API_KEY      = os.getenv("LASTFM_API_KEY", "")
LASTFM_BASE_URL     = "https://ws.audioscrobbler.com/2.0/"

# Nombre de chansons à enrichir par run
# (Last.fm = 5 req/sec → 300/min → ~18K/heure)
# Commence avec 500 pour tester, augmente ensuite
ENRICH_LIMIT   = int(os.getenv("ENRICH_LIMIT", 500))

# Délai entre requêtes pour respecter le rate limit (0.2s = 5 req/sec)
REQUEST_DELAY  = 0.2

# Nombre de tags Last.fm à garder par chanson
MAX_TAGS = 5


# ══════════════════════════════════════════════════════════════
# LAST.FM API
# ══════════════════════════════════════════════════════════════

class LastFmClient:
    """
    Client minimaliste pour l'API Last.fm.
    Gère les erreurs, retries, et rate limiting.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError(
                "LASTFM_API_KEY manquant dans .env\n"
                "→ Crée une clé gratuite sur https://www.last.fm/api/account/create"
            )
        self.api_key = api_key
        self.session = requests.Session() # create a persistent session object : initialisation of the session 
        self.session.headers.update({"User-Agent": "spotify-bigdata-pipeline/1.0"})

    def _get(self, method: str, params: dict, retries: int = 3) -> dict | None:
        """
        Appel générique à l'API Last.fm.
        Retourne le dict JSON ou None en cas d'erreur.
        """
        params.update({
            "method":  method,
            "api_key": self.api_key,
            "format":  "json"
        })

        for attempt in range(retries):
            try:
                #Sending the HTTP request
                response = self.session.get(LASTFM_BASE_URL, params=params, timeout=10)

                # Rate limit dépassé → attendre et réessayer
                if response.status_code == 429: # 429 -> too many requests
                    # number of wait depends on the number of attempt
                    wait = 2 ** attempt
                    log.warning(f"Rate limit Last.fm → attente {wait}s")
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    log.debug(f"Last.fm HTTP {response.status_code} pour {params}")
                    return None

                data = response.json()

                # Last.fm retourne un champ "error" même en HTTP 200
                if "error" in data:
                    log.debug(f"Last.fm erreur {data['error']} : {data.get('message')}")
                    return None

                return data

            except requests.RequestException as e:
                log.debug(f"Erreur réseau Last.fm (tentative {attempt+1}) : {e}")
                time.sleep(1)

        return None

    def get_track_info(self, artist: str, title: str) -> dict | None:
        """
        Récupère les infos d'une chanson : tags, playcount, listeners.
        Doc : https://www.last.fm/api/show/track.getInfo
        """
        data = self._get("track.getInfo", { # adhoma el params
            "artist":      artist,
            "track":       title,
            "autocorrect": 1    # Last.fm corrige les petites fautes de frappe
        })
        if data:
            return data.get("track")
        return None

    def get_artist_top_tags(self, artist: str) -> list[str]:
        """
        Récupère les top tags d'un artiste (fallback si track non trouvé).
        Doc : https://www.last.fm/api/show/artist.getTopTags
        """
        data = self._get("artist.getTopTags", {
            "artist":      artist,
            "autocorrect": 1
        })
        if not data:
            return []
        tags = data.get("toptags", {}).get("tag", [])
        return [t["name"].lower() for t in tags[:MAX_TAGS]]

    def get_similar_artists(self, artist: str, limit: int = 3) -> list[str]:
        """
        Récupère les artistes similaires (utile pour recommandation ALS Couche 3a).
        Doc : https://www.last.fm/api/show/artist.getSimilar
        """
        data = self._get("artist.getSimilar", {
            "artist":      artist,
            "limit":       limit,
            "autocorrect": 1
        })
        if not data:
            return []
        similar = data.get("similarartists", {}).get("artist", [])
        return [a["name"] for a in similar]


# ══════════════════════════════════════════════════════════════
# PARSING + CONSTRUCTION DES MESSAGES
# ══════════════════════════════════════════════════════════════

def parse_track_info(track_data: dict) -> dict:
    """
    Extrait les champs utiles de la réponse track.getInfo de Last.fm.
    """
    if not track_data:
        return {}

    # Tags de genre (ex: ["rock", "alternative", "indie"])
    raw_tags = track_data.get("toptags", {}).get("tag", [])
    tags = [t["name"].lower() for t in raw_tags[:MAX_TAGS]]

    return {
        "lastfm_playcount":  int(track_data.get("playcount", 0) or 0),
        "lastfm_listeners":  int(track_data.get("listeners", 0) or 0),
        "lastfm_tags":       tags, #here are teh tags in track_info
        "lastfm_url":        track_data.get("url", ""),
        # Durée depuis Last.fm (en ms, pour croiser avec songs.csv)
        "lastfm_duration_ms": int(track_data.get("duration", 0) or 0),
    }


def build_metadata_message(song_id: str, artist: str, title: str,
                            track_info: dict, similar_artists: list) -> dict:
    """
    Message enrichi pour le topic song-metadata.
    """
    return {
        "song_id":           song_id,
        "title":             title,
        "artist":            artist,
        "source":            "lastfm_enrichment",
        # Données réelles Last.fm
        "lastfm_playcount":  track_info.get("lastfm_playcount", 0),
        "lastfm_listeners":  track_info.get("lastfm_listeners", 0),
        "lastfm_tags":       track_info.get("lastfm_tags", []),
        "lastfm_url":        track_info.get("lastfm_url", ""),
        "similar_artists":   similar_artists,
        "enriched_at":       pd.Timestamp.utcnow().isoformat()
    }


# ══════════════════════════════════════════════════════════════
# PRODUCER
# ══════════════════════════════════════════════════════════════

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3
    )

# Its job is to take a raw CSV file and turn it into a clean list of songs ready for API enrichment.
def load_songs_to_enrich(csv_path: str, limit: int) -> list[dict]:
    """
    Charge les chansons à enrichir depuis songs.csv.
    Retourne une liste de { song_id, title, artist }.
    """
    log.info(f"Chargement de {csv_path} ...")
    df = pd.read_csv(csv_path, low_memory=False)
    # “Delete any row where id, name, or artists is missing (NaN).”
    df = df.dropna(subset=["id", "name", "artists"])

    if len(df) > limit:
        df = df.sample(n=limit, random_state=42)

    songs = []
    for _, row in df.iterrows():
        try:
            artists_list = ast.literal_eval(str(row["artists"]))
        except Exception:
            artists_list = [str(row["artists"])]

        songs.append({
            "song_id": str(row["id"]),
            "title":   str(row["name"]),
            "artist":  artists_list[0] if artists_list else "Unknown"
        })

    log.info(f"→ {len(songs)} chansons à enrichir")
    return songs


def run():
    """
    Pipeline d'enrichissement :
      Pour chaque chanson dans songs.csv :
        1. Appel Last.fm track.getInfo  → tags, playcount, listeners
        2. Si pas de tags → appel artist.getTopTags (fallback)
        3. Appel artist.getSimilar      → artistes similaires
        4. Envoie dans Kafka : song-metadata 
    """
    api_key = os.getenv("LASTFM_API_KEY", "")
    if not api_key:
        log.error("LASTFM_API_KEY manquant dans .env")
        log.error("→ https://www.last.fm/api/account/create (gratuit)")
        return

    songs_path = os.path.join(DATA_DIR, TRACKS_CSV)
    if not os.path.exists(songs_path):
        log.error(f"songs.csv non trouvé : {songs_path}")
        return

    client   = LastFmClient(api_key)
    producer = create_producer()
    songs    = load_songs_to_enrich(songs_path, ENRICH_LIMIT)

    log.info(f"Démarrage enrichissement Last.fm → {len(songs)} chansons")

    stats = {"enriched": 0, "fallback": 0, "not_found": 0, "errors": 0}

    # Cache des tags artiste pour éviter des appels répétés
    # (plusieurs chansons du même artiste → 1 seul appel API)
    artist_tags_cache:   dict[str, list] = {}
    artist_similar_cache: dict[str, list] = {}

    for i, song in enumerate(songs):
        song_id = song["song_id"]
        artist  = song["artist"] # the primary artist
        title   = song["title"]

        try:
            # ── 1. Track info ────────────────────────────────────
            track_data = client.get_track_info(artist, title)
            track_info = parse_track_info(track_data) if track_data else {}
            # our tags 
            track_tags = track_info.get("lastfm_tags", [])

            if track_data and track_tags:
                stats["enriched"] += 1 # full success
            elif track_data:
                stats["fallback"] += 1
            else:
                stats["not_found"] += 1

            # ── 2. Tags artiste (fallback + cache) ───────────────
            if artist not in artist_tags_cache:
                artist_tags_cache[artist] = client.get_artist_top_tags(artist)
                time.sleep(REQUEST_DELAY)
            artist_tags = artist_tags_cache[artist]

            # ── 3. Artistes similaires (cache) ───────────────────
            if artist not in artist_similar_cache:
                artist_similar_cache[artist] = client.get_similar_artists(artist)
                time.sleep(REQUEST_DELAY)
            similar = artist_similar_cache[artist]

            # ── 4. Envoie Kafka ──────────────────────────────────
            meta = build_metadata_message(song_id, artist, title,
                                          track_info, similar)
            producer.send(TOPIC_SONG_METADATA, value=meta,
                          key=song_id.encode("utf-8"))

            # Rate limiting
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            log.error(f"Erreur sur {title} — {artist} : {e}")
            stats["errors"] += 1
            continue

        # Log de progression toutes les 50 chansons
        if (i + 1) % 50 == 0:
            log.info(
                f"[{i+1}/{len(songs)}] "
                f"enriched={stats['enriched']} "
                f"fallback={stats['fallback']} "
                f"not_found={stats['not_found']} "
                f"errors={stats['errors']}"
            )
            producer.flush()

    producer.flush()
    producer.close()

    log.info("═══════════════════════════════════")
    log.info(f"Enrichissement terminé")
    log.info(f"  ✓ Tags track trouvés : {stats['enriched']}")
    log.info(f"  ~ Tags artiste (fallback) : {stats['fallback']}")
    log.info(f"  ✗ Non trouvés : {stats['not_found']}")
    log.info(f"  ! Erreurs : {stats['errors']}")
    log.info("═══════════════════════════════════")


if __name__ == "__main__":
    run()