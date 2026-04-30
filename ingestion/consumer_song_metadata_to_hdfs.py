"""
consumer_song_metadata_to_hdfs.py
=================================
One-shot Kafka consumer that persists song-metadata into HDFS using
immutable snapshots plus a history index.

This service is the batch persistence layer between Kafka ingestion and Spark K-Means training.
Important :it creates only one snapshot per run
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer


load_dotenv()


@dataclass
class AppConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    consumer_timeout_ms: int
    hdfs_webhdfs_base_url: str
    hdfs_dataset_dir: str
    hdfs_snapshots_dir: str
    hdfs_index_file: str
    group_id: str
    run_id: str


def load_config() -> AppConfig:
    run_id = os.getenv(
        "HDFS_SNAPSHOT_RUN_ID",
        datetime.now(timezone.utc).strftime("snapshot_%Y%m%dT%H%M%SZ"),
    )

    return AppConfig(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("TOPIC_SONG_METADATA", "song-metadata"),
        consumer_timeout_ms=int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "10000")),
        hdfs_webhdfs_base_url=os.getenv("HDFS_WEBHDFS_BASE_URL", "http://localhost:9870"),
        hdfs_dataset_dir=os.getenv("HDFS_DATASET_DIR", "/data/song-metadata"),
        hdfs_snapshots_dir=os.getenv("HDFS_SNAPSHOTS_DIR", "snapshots"),
        hdfs_index_file=os.getenv("HDFS_INDEX_FILE", "_index.json"),
        group_id=os.getenv("HDFS_SNAPSHOT_CONSUMER_GROUP", "song-metadata-hdfs-snapshot"),
        run_id=run_id,
    )


def build_consumer(cfg: AppConfig) -> KafkaConsumer:
    return KafkaConsumer(
        cfg.kafka_topic,    # Topic to subscribe to
        bootstrap_servers=cfg.kafka_bootstrap_servers,  # Kafka cluster address
        auto_offset_reset="earliest",# Start from beginning if no offset exists : très important
        enable_auto_commit=False,# We manually commit offsets (more control) a7na na3mlou commit mba3d manthabtou omourna
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        consumer_timeout_ms=cfg.consumer_timeout_ms, # Stop consumer after timeout if no messages
        group_id=cfg.group_id,
    )


def webhdfs_path(path: str) -> str:
    normalized = "/" + path.lstrip("/") #remove leading slash if exists, then add one to ensure it starts with a single slash
    return quote(normalized, safe="/") #It converts unsafe characters into URL-safe format.

# 📁 Ensure HDFS directory exists
def ensure_webhdfs_directory(cfg: AppConfig, directory: str) -> None:
    url = f"{cfg.hdfs_webhdfs_base_url}/webhdfs/v1{webhdfs_path(directory)}"
    response = requests.put(url, params={"op": "MKDIRS"}, timeout=30)    # Send request to create directory in HDFS
    response.raise_for_status()


def read_webhdfs_file(cfg: AppConfig, file_path: str) -> str | None:
    url = f"{cfg.hdfs_webhdfs_base_url}/webhdfs/v1{webhdfs_path(file_path)}"
    
    # Step 1: Ask NameNode — don't follow redirect automatically
    response = requests.get(
        url,
        params={"op": "OPEN"},
        allow_redirects=False,  # ✅ prevent auto-follow to Docker internal host
        timeout=30,
    )

    if response.status_code == 404:
        return None

    if response.status_code == 200:
        return response.text  # direct response, no redirect needed

    if response.status_code != 307:
        response.raise_for_status()

    # Step 2: Rewrite internal Docker hostname → localhost
    target_url = response.headers.get("Location")
    parsed = urlparse(target_url)
    namenode_host = urlparse(cfg.hdfs_webhdfs_base_url).hostname
    fixed_url = urlunparse(parsed._replace(netloc=f"{namenode_host}:{parsed.port}"))

    # Step 3: Follow the fixed redirect
    read_response = requests.get(fixed_url, timeout=30)
    if read_response.status_code == 404:
        return None
    read_response.raise_for_status()
    return read_response.text

def create_webhdfs_file(cfg: AppConfig, file_path: str, content: str, overwrite=False) -> None:
    url = f"{cfg.hdfs_webhdfs_base_url}/webhdfs/v1{webhdfs_path(file_path)}"
    response = requests.put(
        url,
        params={"op": "CREATE", "overwrite": str(overwrite).lower()},
        allow_redirects=False,
        timeout=30,
    )

    if response.status_code == 201:
        return  # ✅ File created in one step, nothing more to do

    if response.status_code != 307:
        response.raise_for_status()  # Unexpected status → raise

    # 307 redirect: rewrite Docker internal hostname → localhost
    target_url = response.headers.get("Location")
    parsed = urlparse(target_url)
    namenode_host = urlparse(cfg.hdfs_webhdfs_base_url).hostname
    fixed_url = urlunparse(parsed._replace(netloc=f"{namenode_host}:{parsed.port}"))

    upload_response = requests.put(fixed_url, data=content.encode("utf-8"), timeout=120)
    upload_response.raise_for_status()


def snapshot_root(cfg: AppConfig) -> PurePosixPath:
    # Base dataset directory in HDFS
    return PurePosixPath(cfg.hdfs_dataset_dir)


def snapshots_dir(cfg: AppConfig) -> PurePosixPath:
    # Subfolder for snapshots
    return snapshot_root(cfg) / cfg.hdfs_snapshots_dir


def index_path(cfg: AppConfig) -> PurePosixPath:
    # Path to index file
    return snapshot_root(cfg) / cfg.hdfs_index_file

"""Kafka behavior:

Case 1 — messages exist
it keeps yielding messages
Case 2 — no new messages
it waits
if nothing arrives for consumer_timeout_ms
=> it raises StopIteration

So my loop ends.
"""
def consume_song_metadata(consumer: KafkaConsumer) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []

    for message in consumer:
        if isinstance(message.value, dict):
            messages.append(message.value)
    print("000000000000000000000000000000000000000000000000xxxxxxxxxxxxxxxxxxxxxxxxxx00000000000000000000000000000000000")

    return messages


def main() -> None:
    cfg = load_config()
    consumer = build_consumer(cfg)

    print(f"[INFO] Kafka source: {cfg.kafka_bootstrap_servers} / topic={cfg.kafka_topic}")
    print(f"[INFO] HDFS WebHDFS endpoint: {cfg.hdfs_webhdfs_base_url}")

    try:
        # first of all na9raw les messages mil topic w nriglohom f  file jsonl
        messages = consume_song_metadata(consumer)
        if not messages:
            print("[INFO] No new song-metadata messages were available; snapshot skipped.")
            return

        base_dir = snapshot_root(cfg)
        snapshots_path = snapshots_dir(cfg)
        # this is the snapshot (location) kafka -> hdfs : jsonl (json line kolloine file file hadgha json wahdou , json = message  => donc ensemble des messages)
        snapshot_file = snapshots_path / f"song-metadata-{cfg.run_id}.jsonl"
        index_file = index_path(cfg)
        # Each Kafka message becomes a JSON line
        serialized = "\n".join(json.dumps(record, ensure_ascii=False, default=str) for record in messages) + "\n"

        # on vérifie les paths
        ensure_webhdfs_directory(cfg, str(base_dir))
        ensure_webhdfs_directory(cfg, str(snapshots_path))
        # hadhiya hiya el operation elli tekteb el script chnowa 9ra des messages fil hdfs
        create_webhdfs_file(cfg, str(snapshot_file), serialized , overwrite=False)
        # bach ta9ra el index file raw
        existing_index_raw = read_webhdfs_file(cfg, str(index_file))
        # tawika nrodouh json
        if existing_index_raw:
            try:
                existing_index = json.loads(existing_index_raw)
            except json.JSONDecodeError:
                existing_index = {}
        else:
            existing_index = {}
        # nzidou teh snapshot to the history in index
        history = existing_index.get("history", []) if isinstance(existing_index, dict) else []
        if not isinstance(history, list):
            history = []

        history.append(
            {
                "run_id": cfg.run_id,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "record_count": len(messages),
                "snapshot_path": str(snapshot_file),
            }
        )

        index_payload = {
            "dataset": "song-metadata",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "history": history,
        }
        # attention :❗ “rebuilding the whole file and replacing it”
        create_webhdfs_file(cfg, str(index_file), json.dumps(index_payload, ensure_ascii=False, indent=2), overwrite=True)
        # lehna nbadlou fil offset 
        consumer.commit()

        print(f"[INFO] Wrote {len(messages)} new song-metadata record(s) to {snapshot_file}")
        print(f"[INFO] Updated history index at {index_file}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()