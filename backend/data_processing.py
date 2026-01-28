import os
import pandas as pd
import re
import boto3
from sqlalchemy import create_engine,text
import streamlit as st
from config import PG_CONFIG,SNOW_CONFIG,AWS_ACCESS_KEY,AWS_SECRET_KEY,AWS_REGION,S3_BUCKET_NAME,S3_FILE_KEY,FOLDER_PATH
from db import df_query,exec_sql

os.environ['PG_HOST']='localhost'
os.environ['POSTGRES_HOST']='localhost'

pg_engine=create_engine(
    f"postgresql://postgres:postgres@localhost:5432/music"
)
snow_engine=create_engine(
    f"snowflake://{SNOW_CONFIG['user']}:{SNOW_CONFIG['password']}@{SNOW_CONFIG['account']}/?warehouse={SNOW_CONFIG['warehouse']}&database={SNOW_CONFIG['database']}&schema={SNOW_CONFIG['schema']}"
)

def read_csv_smart(path):
    encodings=["utf-8-sig","utf-8","cp949","euc-kr"]
    seps=[",","\t",";","|"]
    for enc in encodings:
        for sep in seps:
            try:
                df=pd.read_csv(path,encoding=enc,sep=sep)
                if df is not None and not df.empty and len(df.columns)>=2:
                    return df
            except:
                continue
    return pd.read_csv(path,encoding="utf-8-sig",sep=None,engine="python")

def _normalize_platform_token(token):
    t=str(token).strip().lower()
    if t in ["youtube","yt"]:
        return "YouTube"
    if t in ["spotify","sp"]:
        return "Spotify"
    if t in ["soundcloud","sc"]:
        return "SoundCloud"
    return None

def _normalize_metric_token(token):
    t=str(token).strip().lower()
    if t in ["views","view"]:
        return "views"
    if t in ["streams","stream"]:
        return "streams"
    if t in ["plays","play"]:
        return "plays"
    return None

def process_and_upload_excel():
    if not os.path.exists(FOLDER_PATH):
        st.sidebar.error(f"Folder not found: {FOLDER_PATH}")
        return
    all_files=[f for f in os.listdir(FOLDER_PATH) if f.lower().endswith(".csv")]
    success_count=0
    error_count=0
    for file_name in all_files:
        path=os.path.join(FOLDER_PATH,file_name)
        file_base=file_name[:-4]
        parts=file_base.split("_")

        if len(parts)<4:
            error_count+=1
            continue

        artist=parts[0].replace(" ","")
        song=parts[1]
        platform=_normalize_platform_token(parts[2])
        metric=_normalize_metric_token(parts[3])

        if not platform or not metric:
            error_count+=1
            continue

        expected_metric="views" if platform=="YouTube" else "streams" if platform=="Spotify" else "plays"
        if metric!=expected_metric:
            error_count+=1
            continue

        try:
            df=read_csv_smart(path)
            df.columns=["Date","Value"]+list(df.columns[2:])
            df["date"]=pd.to_datetime(df["Date"].astype(str).apply(lambda x:"".join(re.findall(r"\d+",x))[:8]),errors="coerce")
            df["value"]=pd.to_numeric(df["Value"],errors="coerce")
            final_df=df[["date","value"]].dropna()
            final_df["artist_name"]=artist
            final_df["song_name"]=song
            final_df["metric_type"]=platform
            with pg_engine.connect() as pg_conn:
                pg_conn.execute(text("DELETE FROM artist_growth_data WHERE artist_name=:a AND song_name=:s AND metric_type=:p"),{"a":artist,"s":song,"p":platform})
                pg_conn.commit()
                final_df.to_sql("artist_growth_data",pg_conn,if_exists="append",index=False)
                exec_sql("INSERT INTO artists (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;",(artist,))
                res_id=df_query("SELECT id FROM artists WHERE name=%s;",(artist,))
                if not res_id.empty:
                    a_id=int(res_id.iloc[0]["id"])
                    col="youtube_views" if platform=="YouTube" else "spotify_streams" if platform=="Spotify" else "soundcloud_plays"
                    for _,row in final_df.groupby("date")["value"].sum().reset_index().iterrows():
                        exec_sql(f"INSERT INTO daily_metrics (artist_id,date,{col}) VALUES (%s,%s,%s) ON CONFLICT (artist_id,date) DO UPDATE SET {col}=EXCLUDED.{col};",(a_id,row["date"].date().isoformat(),int(row["value"])))
            success_count+=1
        except:
            error_count+=1
    st.sidebar.info(f"Sync complete: {success_count} success, {error_count} failed")

def delete_artist_and_data(artist_name):
    try:
        res=df_query("SELECT id FROM artists WHERE name=%s;",(artist_name,))
        if res.empty:
            return False
        a_id=int(res.iloc[0]["id"])
        exec_sql("DELETE FROM daily_metrics WHERE artist_id=%s;",(a_id,))
        exec_sql("DELETE FROM artist_growth_data WHERE artist_name=%s;",(artist_name,))
        exec_sql("DELETE FROM artists WHERE id=%s;",(a_id,))
        return True
    except:
        return False

@st.cache_data(ttl=300)
def get_lyrics_from_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_KEY)
        content = response["Body"].read().decode("utf-8")

        songs = []
        current_song = None
        current_lyrics = []

        for line in content.split("\n"):
            line = line.strip()
            if " - " in line and line.endswith(":"):
                if current_song and current_lyrics:
                    songs.append({**current_song, "review": "\n".join(current_lyrics)[:500]})
                parts = line[:-1].split(" - ", 1)
                current_song = {"artist": parts[0].strip(), "title": parts[1].strip()}
                current_lyrics = []
            elif current_song and line:
                current_lyrics.append(line)

        if current_song and current_lyrics:
            songs.append({**current_song, "review": "\n".join(current_lyrics)[:500]})

        return songs[:5]

    except Exception as e:
        st.sidebar.error(f"S3 가사 로드 실패: {type(e).__name__}: {e}")
        return []
