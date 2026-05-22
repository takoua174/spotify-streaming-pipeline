import os
import json
import time
import logging
import ast
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer_search_documents")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SEARCH = os.getenv("TOPIC_SONG_SEARCH", "song-search-documents")
DATA_DIR = os.getenv("DATA_DIR", "./data")
SONGS_CSV = os.getenv("SONGS_CSV", "songs.csv")

BATCH_SIZE = int(os.getenv("SEARCH_BATCH_SIZE", "500"))
BATCH_PAUSE_SEC = float(os.getenv("SEARCH_BATCH_PAUSE_SEC", "0.1"))


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=10,
    )


def parse_list_field(raw) -> list:
    if pd.isna(raw) if not isinstance(raw, list) else False:
        return []
    if isinstance(raw, list):
        return raw
    try:
        return ast.literal_eval(raw)
    except Exception:
        try:
            return json.loads(raw)
        except Exception:
            return [str(raw)] if raw else []


def safe_str(val) -> str:
    """Return empty string for NaN/None, otherwise cast to str."""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val) if val is not None else ""


def safe_int(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def build_search_event(row: dict) -> dict:
    artists = parse_list_field(row.get("artists", "[]"))
    niche = parse_list_field(row.get("niche_genres", "[]"))
    primary = artists[0] if artists else "Unknown"

    return {
        "song_id":        safe_str(row.get("id", "")),
        "title":          safe_str(row.get("name", "")),
        "artists":        artists,
        "primary_artist": primary,
        "lyrics":         safe_str(row.get("lyrics", "")),
        "genre":          safe_str(row.get("genre", "")),
        "niche_genres":   niche,
        "popularity":     safe_int(row.get("popularity")),
        "year":           safe_int(row.get("year")),
        "source":         "kaggle_songs_csv",
    }


def run():
    songs_path = os.path.join(DATA_DIR, SONGS_CSV)
    if not os.path.exists(songs_path):
        log.error(f"songs.csv not found at {songs_path}")
        return

    log.info(f"Starting producer -> broker={BOOTSTRAP_SERVERS} topic={TOPIC_SEARCH}")
    producer = create_producer()

    # FIX: use to_dict("records") instead of iterrows() — 3-5x faster on 550K rows
    df = pd.read_csv(songs_path, low_memory=False)
    records = df.to_dict(orient="records")
    total = len(records)
    sent = 0

    try:
        for row_d in records:
            event = build_search_event(row_d)

            if not event["song_id"]:
                log.warning("Skipping row with empty song_id")
                continue

            producer.send(
                TOPIC_SEARCH,
                value=event,
                key=event["song_id"].encode("utf-8"),
            )
            sent += 1

            if sent % BATCH_SIZE == 0:
                producer.flush()
                pct = sent * 100 // total if total else 0
                log.info(f"{sent}/{total} messages sent ({pct}%)")
                time.sleep(BATCH_PAUSE_SEC)

        producer.flush()
        log.info(f"Done: {sent}/{total} messages sent to '{TOPIC_SEARCH}'")

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    except Exception:
        log.exception("Producer error")
        raise
    finally:
        producer.close()
        log.info("Producer closed.")


if __name__ == "__main__":
    run()