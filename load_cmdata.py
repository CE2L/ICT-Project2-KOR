import os, re, glob
import pandas as pd
import psycopg2

CM_PATH = "/home/azureuser/project1/backend/cmdata"

def split_filename(fp):
    base = os.path.splitext(os.path.basename(fp))[0]
    parts = base.split("_")
    if len(parts) < 4:
        return None
    artist_raw = parts[0]
    track_raw = parts[1]
    platform_raw = parts[2]
    metric_raw = parts[3]
    return artist_raw, track_raw, platform_raw, metric_raw

def pretty_artist(s):
    s = str(s).strip()
    s = re.sub(r"(?<!^)([A-Z])", r" \1", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def metric_type_from(platform_raw, metric_raw):
    p = str(platform_raw).strip().lower()
    m = str(metric_raw).strip().lower()
    if "soundcloud" in p:
        return "soundcloud_plays"
    if "spotify" in p:
        return "spotify_streams"
    if "youtube" in p:
        return "youtube_views"
    if "plays" in m:
        return "soundcloud_plays"
    if "streams" in m:
        return "spotify_streams"
    if "views" in m:
        return "youtube_views"
    return f"{p}_{m}".replace("-", "_").replace(" ", "_")

def detect_cols(df):
    cols = [str(c).strip() for c in df.columns]
    lower = {c.lower(): c for c in cols}

    date_col = None
    for k in ["날짜", "date", "day", "ds", "timestamp", "time"]:
        if k in cols:
            date_col = k
            break
        if k in lower:
            date_col = lower[k]
            break
    if date_col is None:
        date_col = cols[0]

    val_col = None
    prefer = ["총 재생 횟수", "총 스트림", "총 조회수", "총 조회", "value", "count", "plays", "streams", "views"]
    for k in prefer:
        if k in cols:
            val_col = k
            break
        if k.lower() in lower:
            val_col = lower[k.lower()]
            break
    if val_col is None:
        val_col = cols[1] if len(cols) > 1 else cols[0]

    return date_col, val_col

def parse_korean_date_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip()
    x = x.str.replace(r"\s*년\s*", "-", regex=True)
    x = x.str.replace(r"\s*월\s*", "-", regex=True)
    x = x.str.replace(r"\s*일\s*", "", regex=True)
    x = x.str.replace(r"\s+", "", regex=True)
    dt = pd.to_datetime(x, errors="coerce", format="%Y-%m-%d")
    if dt.isna().all():
        dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date

host = os.getenv("PG_HOST", "127.0.0.1")
port = int(os.getenv("PG_PORT", "5432"))
user = os.getenv("PG_USER", "postgres")
pw   = os.getenv("PG_PASSWORD", "postgres")
db   = os.getenv("PG_DB", "music")

files = sorted(glob.glob(os.path.join(CM_PATH, "*.csv")))
print("csv:", len(files))
if not files:
    raise SystemExit("no csv files found")

conn = psycopg2.connect(host=host, port=port, user=user, password=pw, dbname=db)
conn.autocommit = False
cur = conn.cursor()

# artist_growth_data가 이미 존재하더라도, 최소 인덱스는 보장
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_artist_growth_unique
ON artist_growth_data (artist_name, metric_type, date);
""")
conn.commit()

rows_growth = 0

for fp in files:
    meta = split_filename(fp)
    if meta is None:
        continue

    artist_raw, track_raw, platform_raw, metric_raw = meta
    artist_name = f"{pretty_artist(artist_raw)} - {track_raw}"
    track_name = str(track_raw).strip()
    mtype = metric_type_from(platform_raw, metric_raw)

    df = pd.read_csv(fp)
    if df is None or df.empty:
        continue

    date_col, val_col = detect_cols(df)
    if date_col not in df.columns or val_col not in df.columns:
        continue

    tmp = df[[date_col, val_col]].copy()
    tmp.columns = ["date", "value"]

    tmp["date"] = parse_korean_date_series(tmp["date"])
    tmp["value"] = pd.to_numeric(tmp["value"], errors="coerce")

    tmp = tmp.dropna(subset=["date", "value"])
    if tmp.empty:
        continue

    cur.execute("INSERT INTO artists(name) VALUES(%s) ON CONFLICT (name) DO NOTHING;", (artist_name,))

    data = [(artist_name, track_name, mtype, r["date"], float(r["value"])) for _, r in tmp.iterrows()]
    cur.executemany(
        """
        INSERT INTO artist_growth_data (artist_name, track_name, metric_type, date, value)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (artist_name, metric_type, date) DO UPDATE SET
          value=EXCLUDED.value,
          track_name=EXCLUDED.track_name;
        """,
        data
    )
    rows_growth += len(data)

conn.commit()
print("artist_growth_data upserts:", rows_growth)

cur.execute("""
INSERT INTO daily_metrics (artist_id, date, youtube_views, spotify_streams, soundcloud_plays)
SELECT
  a.id as artist_id,
  g.date as date,
  MAX(CASE WHEN g.metric_type='youtube_views' THEN g.value ELSE NULL END)::bigint as youtube_views,
  MAX(CASE WHEN g.metric_type='spotify_streams' THEN g.value ELSE NULL END)::bigint as spotify_streams,
  MAX(CASE WHEN g.metric_type='soundcloud_plays' THEN g.value ELSE NULL END)::bigint as soundcloud_plays
FROM artist_growth_data g
JOIN artists a ON a.name = g.artist_name
GROUP BY a.id, g.date
ON CONFLICT (artist_id, date) DO UPDATE SET
  youtube_views = COALESCE(EXCLUDED.youtube_views, daily_metrics.youtube_views),
  spotify_streams = COALESCE(EXCLUDED.spotify_streams, daily_metrics.spotify_streams),
  soundcloud_plays = COALESCE(EXCLUDED.soundcloud_plays, daily_metrics.soundcloud_plays);
""")
rows_daily = cur.rowcount
conn.commit()
print("daily_metrics upserts:", rows_daily)

cur.close()
conn.close()
print("done")
