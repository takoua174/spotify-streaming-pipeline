-- storage/cassandra/init.cql
CREATE KEYSPACE IF NOT EXISTS spotify_streaming 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE spotify_streaming;

-- Table pour le détecteur de tendances (trend_detector)
CREATE TABLE IF NOT EXISTS trending_songs (
    window_start timestamp,
    window_end timestamp,
    song_id text,
    title text,
    artist text,
    play_count int,
    PRIMARY KEY ((window_start, window_end), play_count, song_id)
) WITH CLUSTERING ORDER BY (play_count DESC);

-- Table pour le tracker de mood (mood_tracker)
CREATE TABLE IF NOT EXISTS mood_index (
    window_start timestamp,
    window_end timestamp,
    artist text, 
    mood_score float,
    avg_valence float,
    avg_energy float,
    PRIMARY KEY ((window_start, window_end), mood_score)
) WITH CLUSTERING ORDER BY (mood_score DESC);

-- Table pour la détection d'anomalies (anomaly_detector)
CREATE TABLE IF NOT EXISTS anomalies (
    window_start timestamp,
    window_end timestamp,
    user_id text,
    anomaly_type text,
    description text,
    PRIMARY KEY ((window_start, window_end), user_id, anomaly_type)
);

-- Dashboard-friendly tables (Grafana)
-- Note: existing tables above are partitioned by window, which is not ideal for
-- time-range queries like "last 15 minutes". These tables are optimized for that.

-- Time series by artist (proxy for genre in current streaming implementation)
CREATE TABLE IF NOT EXISTS mood_index_by_artist (
    artist text,
    window_start timestamp,
    window_end timestamp,
    mood_score float,
    avg_valence float,
    avg_energy float,
    PRIMARY KEY ((artist), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Recent trending feed (single partition for demo/monitoring)
CREATE TABLE IF NOT EXISTS trending_songs_monitor (
    bucket text,
    window_start timestamp,
    window_end timestamp,
    song_id text,
    title text,
    artist text,
    play_count int,
    PRIMARY KEY ((bucket), window_start, song_id)
) WITH CLUSTERING ORDER BY (window_start DESC, song_id ASC);

-- Recent anomalies feed (single partition for demo/monitoring)
CREATE TABLE IF NOT EXISTS anomalies_monitor (
    bucket text,
    window_start timestamp,
    window_end timestamp,
    user_id text,
    anomaly_type text,
    description text,
    PRIMARY KEY ((bucket), window_start, user_id, anomaly_type)
) WITH CLUSTERING ORDER BY (window_start DESC, user_id ASC, anomaly_type ASC);
