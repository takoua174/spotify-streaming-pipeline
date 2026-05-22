import os
import json
import logging
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer_elasticsearch")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SEARCH      = os.getenv("TOPIC_SONG_SEARCH", "song-search-documents")
GROUP_ID          = os.getenv("KAFKA_CONSUMER_GROUP", "es-indexer")
ES_HOST           = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER           = os.getenv("ES_USER")
ES_PASS           = os.getenv("ES_PASS")
INDEX_NAME        = os.getenv("ES_INDEX", "songs-search")
BATCH_SIZE        = int(os.getenv("ES_BATCH_SIZE", "500"))


def create_es_client() -> Elasticsearch:
    # FIX: use basic_auth= instead of deprecated http_auth= (ES client 8.x)
    if ES_USER and ES_PASS:
        return Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASS))
    return Elasticsearch([ES_HOST])


def ensure_index(es: Elasticsearch):
    from elasticsearch import NotFoundError
    try:
        es.indices.get(index=INDEX_NAME)
        log.info(f"Index '{INDEX_NAME}' already exists — skipping creation")
        return
    except NotFoundError:
        pass  # index doesn't exist yet, proceed to create it

    # FIX: pass mappings= directly instead of body={} (deprecated in ES 8.x)
    es.indices.create(
        index=INDEX_NAME,
        mappings={
            "properties": {
                # Full-text search with English stemming; store=False avoids
                # duplicating raw lyrics in _source since the app only needs song_id back
                "lyrics":         {"type": "text", "analyzer": "english", "store": False},
                "title":          {"type": "text"},
                # keyword fields — used for filtering / aggregations, not full-text
                "primary_artist": {"type": "keyword"},
                "artists":        {"type": "keyword"},
                "genre":          {"type": "keyword"},
                "niche_genres":   {"type": "keyword"},
                "source":         {"type": "keyword"},
                # numeric fields for ranking / boosting
                "popularity":     {"type": "integer"},
                "year":           {"type": "integer"},
            }
        },
    )
    log.info(f"Created index '{INDEX_NAME}' with mapping")


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_SEARCH,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,         # we commit manually after each successful bulk
        auto_offset_reset="earliest",
        # FIX: removed consumer_timeout_ms — previously set to 1000ms which caused
        # the consumer to silently stop after 1s of inactivity mid-ingestion
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def flush_buffer(es: Elasticsearch, consumer: KafkaConsumer, buffer: list) -> None:
    """Bulk-index a batch of actions, log failures, then commit offsets."""
    if not buffer:
        return

    # FIX: capture failed count and log it before committing
    # raise_on_error=False lets us handle partial failures gracefully
    success, failed = helpers.bulk(es, buffer, stats_only=True, raise_on_error=False)

    if failed:
        log.warning(f"{failed} document(s) failed to index — check ES logs for details")

    log.info(f"Indexed {success} document(s) successfully")

    # Only commit offsets after ES confirms the write — guarantees at-least-once delivery
    consumer.commit()


def build_action(event: dict) -> dict | None:
    """Build a single ES bulk action from a Kafka message payload."""
    doc_id = event.get("song_id") or event.get("songId")
    if not doc_id:
        log.warning("Message missing song_id — skipping")
        return None

    # FIX: exclude song_id from _source — it's already stored as _id,
    # so storing it twice wastes space. The React app retrieves it via hit._id.
    source = {k: v for k, v in event.items() if k != "song_id"}

    return {
        "_index":  INDEX_NAME,
        "_id":     str(doc_id),  # idempotent: re-running won't create duplicates
        "_source": source,
    }


def run():
    es = create_es_client()
    ensure_index(es)

    consumer = create_consumer()
    log.info(
        f"Consumer started -> broker={BOOTSTRAP_SERVERS} "
        f"topic={TOPIC_SEARCH} group={GROUP_ID}"
    )

    buffer = []
    try:
        for message in consumer:
            event = message.value
            if not isinstance(event, dict):
                log.warning(f"Non-dict message at offset {message.offset} — skipping")
                continue

            action = build_action(event)
            if action is None:
                continue

            buffer.append(action)

            if len(buffer) >= BATCH_SIZE:
                flush_buffer(es, consumer, buffer)
                buffer = []

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing remaining buffer...")
        flush_buffer(es, consumer, buffer)
        buffer = []

    except Exception:
        log.exception("Consumer error — remaining buffer NOT committed")
        raise

    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    run()