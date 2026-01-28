CREATE TABLE IF NOT EXISTS artists (
  artist_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  mbid TEXT,
  url TEXT,
  UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS daily_metrics (
  metric_date DATE NOT NULL,
  artist_id INT NOT NULL REFERENCES artists(artist_id) ON DELETE CASCADE,
  listeners_total BIGINT NOT NULL,
  playcount_total BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), 
  PRIMARY KEY (metric_date, artist_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_artist_date
  ON daily_metrics (artist_id, metric_date);