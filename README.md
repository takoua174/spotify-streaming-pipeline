# spotify-streaming-pipeline

End-to-end big data pipeline for music analytics — Kafka ingestion, Spark batch & streaming, polyglot storage (Cassandra / PostgreSQL / Elasticsearch), and real-time dashboards. Built on the Kaggle 550K Spotify dataset.

---

## Architecture overview

```
Sources de données
  ├── Python event simulator  (play · skip · like)
  ├── External APIs           (genre tags · lyrics enrichment)
  └── Kaggle CSV dataset      (550K tracks)
          │
          ▼
Apache Kafka  ──  5 topics
  song-plays · user-events · song-metadata · mood-index · genre-signals
          │
          ├──▶  Stream Processing  (Spark Structured Streaming)
          │       mood tracker · trend detector · anomaly detection
          │
          └──▶  Batch Processing  (Apache Spark — nightly)
                  ALS recommendations · Sentiment NLP · K-Means clustering
                          │
                          ▼
              Polyglot Storage
                Cassandra · PostgreSQL · Elasticsearch
                          │
                          ▼
                        Visualisation
                Grafana · Metabase · React Web App
```

---

## Tech stack

| Layer             | Technology                             |
| ----------------- | -------------------------------------- |
| Ingestion         | Python · kafka-python                  |
| Message bus       | Apache Kafka 7.5 · Zookeeper           |
| Batch processing  | Apache Spark 3.x · MLlib · Spark NLP   |
| Stream processing | Spark Structured Streaming             |
| Storage           | Cassandra · PostgreSQL · Elasticsearch |
| Visualisation     | Grafana · Metabase · React             |
| Infrastructure    | Docker · Docker Compose                |

---

## Dataset

[Kaggle — 550K Spotify Songs: Audio, Lyrics and Genres](https://www.kaggle.com/datasets/serkantysz/550k-spotify-songs-audio-lyrics-and-genres)

Download and place the CSV files in `./data/` before running.

---

## Getting started

```bash
# 1. Clone
git clone https://github.com/your-username/spotify-streaming-pipeline
cd spotify-streaming-pipeline

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start Kafka
cd docker && docker compose up -d

# 4. Run the event simulator
python ingestion/producer_stream.py

# 5. Verify messages are flowing
python ingestion/test_consumer.py

# 6. (After placing tracks.csv in ./data/) load the dataset
python ingestion/producer_batch_csv.py
```

Kafka UI available at `http://localhost:8080` once the stack is running.

---

## Layer 3a — K-Means genre clustering (implemented)

This batch job reads song audio metadata from HDFS snapshot files,
evaluates candidate K values, trains a final K-Means model on the full
dataset, then writes dashboard-ready outputs to PostgreSQL and publishes
clustering signals to Kafka topic `genre-signals`.

### New files

```text
processing/batch/kmeans_genre_clustering.py
storage/postgres/init/001_kmeans_schema.sql
```

### Run order

```bash
# 1) Start infrastructure (Kafka + Spark + PostgreSQL)
cd docker
docker compose up -d --build

# 2) Feed Kafka topic song-metadata with batch producer
cd ..
python ingestion/producer_batch_csv.py

# 3) Materialize an immutable HDFS snapshot from Kafka
docker compose exec song-metadata-hdfs-snapshot /bin/bash -lc "python ingestion/consumer_song_metadata_to_hdfs.py"

# 4) Verify the HDFS snapshot files and index
docker compose exec hdfs-namenode /bin/bash -lc "hdfs dfs -ls /data/song-metadata && hdfs dfs -ls /data/song-metadata/snapshots"
docker compose exec hdfs-namenode /bin/bash -lc "hdfs dfs -cat /data/song-metadata/_index.json"

# 5) Submit Spark batch job from Spark master container
cd docker
docker compose exec spark-master /bin/bash -lc "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars ; /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.4 /opt/pipeline/processing/batch/kmeans_genre_clustering.py"
```

### Output tables (PostgreSQL)

- `analytics.batch_runs`
- `analytics.cluster_metrics`
- `analytics.song_clusters`
- `analytics.cluster_centroids`
- `analytics.cluster_profiles`
- `analytics.v_cluster_overview`

### Kafka output (from Spark batch)

- Topic: `genre-signals`
- Message types: `song_cluster_assignment`, `cluster_profile`

### Inspect K-Means results in Kafka

Use Kafka UI at `http://localhost:8080`, or consume the topic directly from the broker:

```bash
cd docker
docker compose exec kafka /bin/bash -lc "kafka-console-consumer --bootstrap-server kafka:29092 --topic genre-signals --from-beginning --timeout-ms 10000"
```

### HDFS batch input

- Root dataset path: `/data/song-metadata`
- Immutable snapshots: `/data/song-metadata/snapshots/song-metadata-<run_id>.jsonl`
- History index: `/data/song-metadata/_index.json`
- The Spark batch job reads all snapshot paths listed in the index and trains on the whole dataset.

### Quick verification

```bash
cd docker
docker compose exec postgres psql -U spotify_user -d spotify_analytics -c "SELECT run_id, final_k, final_silhouette FROM analytics.batch_runs ORDER BY created_at DESC LIMIT 5;"
docker compose exec postgres psql -U spotify_user -d spotify_analytics -c "SELECT run_id, cluster_id, cluster_size FROM analytics.cluster_profiles ORDER BY run_id DESC, cluster_id;"
```

---

## Project structure

```
spotify-streaming-pipeline/
├── docker/
│   └── docker-compose.yml       # Kafka + Zookeeper + Kafka UI
├── ingestion/
│   ├── producer_stream.py       # Simulated play/skip/like events
│   ├── producer_enrichment.py   # External API enrichment
│   ├── producer_batch_csv.py    # Kaggle CSV → Kafka
│   └── test_consumer.py         # Topic verification tool
├── processing/
│   ├── batch/                   # Spark batch jobs (layer 3a)
│   └── stream/                  # Spark streaming jobs (layer 3b)
├── storage/
│   ├── cassandra/               # Schema + connectors
│   ├── postgres/                # Schema + migrations
│   └── elasticsearch/           # Index mappings
├── dashboard/
│   ├── grafana/                 # Dashboard provisioning
│   └── react-app/               # Mood Map + recommendations UI
├── data/                        # Place Kaggle CSV files here
├── requirements.txt
├── .env
└── README.md
```

---

## Development roadmap

- [x] Layer 1 — Kafka ingestion (producers + topics)
- [ ] Layer 2 — Topic validation & consumer tests (leave it later)
- [ ] Layer 3a — Spark batch (ALS · NLP · K-Means)
- [ ] Layer 3b — Spark streaming (mood tracker · anomaly detection)
- [ ] Layer 4 — Polyglot storage (Cassandra · PostgreSQL · Elasticsearch)
- [ ] Layer 5 — Visualisation (Grafana · Metabase · React)

---

## License

MIT
