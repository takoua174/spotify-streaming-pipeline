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
- [ ] Layer 2 — Topic validation & consumer tests
- [ ] Layer 3a — Spark batch (ALS · NLP · K-Means)
- [ ] Layer 3b — Spark streaming (mood tracker · anomaly detection)
- [ ] Layer 4 — Polyglot storage (Cassandra · PostgreSQL · Elasticsearch)
- [ ] Layer 5 — Visualisation (Grafana · Metabase · React)

---

## License

MIT

Layer 1:

# ── 1. Installer les dépendances ────────────────────────────

pip install -r requirements.txt

# ── 2. Lancer Kafka + Zookeeper ─────────────────────────────

cd docker
docker compose up -d

# Attendre ~30 secondes que Kafka soit prêt

# Vérifier que tout tourne

docker compose ps

# → kafka et zookeeper doivent être "Up"

# ── 3. Vérifier les topics créés ────────────────────────────

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# ── 4. Lancer le simulateur d'événements (dans un terminal) ─

cd ..
python ingestion/producer_stream.py

# ── 5. Dans un 2ème terminal : vérifier les messages ────────

python ingestion/test_consumer.py

# ── 6. (Optionnel) Kafka UI dans ton navigateur ─────────────

# Ouvrir : http://localhost:8080

# → Tu verras les topics et les messages en temps réel

# ── 7. (Après avoir mis tracks.csv dans ./data/) ─────────────

python ingestion/producer_batch_csv.py
