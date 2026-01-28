CREATE TABLE IF NOT EXISTS artists (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  mbid TEXT,
  url TEXT,
  yt_channel_id TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_metrics (
  id SERIAL PRIMARY KEY,
  artist_id INTEGER NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
  date DATE NOT NULL,
  playcount BIGINT,
  listeners BIGINT,
  yt_subs BIGINT,
  yt_views BIGINT,
  yt_avg_eng_ratio FLOAT,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (artist_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_artist_date 
  ON daily_metrics(artist_id, date DESC);
```

```
LASTFM_API_KEY=your_key
OPENAI_API_KEY=your_key
GOOGLE_API_KEY=your_key
GITHUB_API_KEY=your_key