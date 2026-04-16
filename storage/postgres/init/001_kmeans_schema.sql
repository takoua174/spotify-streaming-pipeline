CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.batch_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    kafka_topic VARCHAR(128) NOT NULL,
    input_rows_raw BIGINT NOT NULL,
    input_rows_dedup BIGINT NOT NULL,
    candidate_ks TEXT NOT NULL,
    recommended_k INTEGER NOT NULL,
    final_k INTEGER NOT NULL,
    final_silhouette DOUBLE PRECISION,
    final_training_cost DOUBLE PRECISION,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.cluster_metrics (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES analytics.batch_runs(run_id),
    evaluation_scope VARCHAR(16) NOT NULL,
    k INTEGER NOT NULL,
    silhouette DOUBLE PRECISION NOT NULL,
    training_cost DOUBLE PRECISION,
    num_clusters_found INTEGER NOT NULL,
    evaluated_on_rows BIGINT NOT NULL,
    is_recommended BOOLEAN NOT NULL DEFAULT FALSE,
    is_final BOOLEAN NOT NULL DEFAULT FALSE,
    evaluated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.song_clusters (
    run_id VARCHAR(64) NOT NULL REFERENCES analytics.batch_runs(run_id),
    song_id TEXT NOT NULL,
    main_genre TEXT,
    genres_json TEXT,
    cluster_id INTEGER NOT NULL,
    danceability DOUBLE PRECISION,
    energy DOUBLE PRECISION,
    acousticness DOUBLE PRECISION,
    instrumentalness DOUBLE PRECISION,
    valence DOUBLE PRECISION,
    speechiness DOUBLE PRECISION,
    liveness DOUBLE PRECISION,
    loudness_norm DOUBLE PRECISION,
    tempo_norm DOUBLE PRECISION,
    assigned_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, song_id)
);

CREATE TABLE IF NOT EXISTS analytics.cluster_centroids (
    run_id VARCHAR(64) NOT NULL REFERENCES analytics.batch_runs(run_id),
    cluster_id INTEGER NOT NULL,
    centroid_danceability DOUBLE PRECISION,
    centroid_energy DOUBLE PRECISION,
    centroid_acousticness DOUBLE PRECISION,
    centroid_instrumentalness DOUBLE PRECISION,
    centroid_valence DOUBLE PRECISION,
    centroid_speechiness DOUBLE PRECISION,
    centroid_liveness DOUBLE PRECISION,
    centroid_loudness_norm DOUBLE PRECISION,
    centroid_tempo_norm DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, cluster_id)
);

CREATE TABLE IF NOT EXISTS analytics.cluster_profiles (
    run_id VARCHAR(64) NOT NULL REFERENCES analytics.batch_runs(run_id),
    cluster_id INTEGER NOT NULL,
    cluster_size BIGINT NOT NULL,
    avg_danceability DOUBLE PRECISION,
    avg_energy DOUBLE PRECISION,
    avg_acousticness DOUBLE PRECISION,
    avg_instrumentalness DOUBLE PRECISION,
    avg_valence DOUBLE PRECISION,
    avg_speechiness DOUBLE PRECISION,
    avg_liveness DOUBLE PRECISION,
    avg_loudness_norm DOUBLE PRECISION,
    avg_tempo_norm DOUBLE PRECISION,
    top_genres_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_cluster_metrics_run_id ON analytics.cluster_metrics(run_id);
CREATE INDEX IF NOT EXISTS idx_song_clusters_cluster_id ON analytics.song_clusters(run_id, cluster_id);
CREATE INDEX IF NOT EXISTS idx_song_clusters_main_genre ON analytics.song_clusters(run_id, main_genre);
CREATE INDEX IF NOT EXISTS idx_cluster_profiles_size ON analytics.cluster_profiles(run_id, cluster_size DESC);

CREATE OR REPLACE VIEW analytics.v_cluster_overview AS
SELECT
    p.run_id,
    p.cluster_id,
    p.cluster_size,
    p.avg_danceability,
    p.avg_energy,
    p.avg_acousticness,
    p.avg_instrumentalness,
    p.avg_valence,
    p.avg_speechiness,
    p.avg_liveness,
    p.avg_loudness_norm,
    p.avg_tempo_norm,
    p.top_genres_json,
    c.centroid_danceability,
    c.centroid_energy,
    c.centroid_acousticness,
    c.centroid_instrumentalness,
    c.centroid_valence,
    c.centroid_speechiness,
    c.centroid_liveness,
    c.centroid_loudness_norm,
    c.centroid_tempo_norm
FROM analytics.cluster_profiles p
JOIN analytics.cluster_centroids c
  ON p.run_id = c.run_id
 AND p.cluster_id = c.cluster_id;
