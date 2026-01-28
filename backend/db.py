import os
import time
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def _env(name: str, default: str = "") -> str:
    """환경 변수를 읽어오며 값이 없을 경우 기본값을 반환합니다."""
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v)


def _required_env(name: str) -> str:
    """필수 환경 변수를 읽어오며 값이 없을 경우 에러를 발생시킵니다."""
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise ValueError(f"필수 환경 변수 누락: {name}. .env 파일을 확인하세요.")
    return str(v)


def _get_pg_config():
    """환경 변수로부터 PostgreSQL 설정 정보를 가져옵니다."""
    host = _required_env("PG_HOST")
    port = int(_env("PG_PORT", "5432"))
    user = _required_env("PG_USER")
    password = _required_env("PG_PASSWORD")
    database = _required_env("PG_DB")
    return host, port, user, password, database


def ensure_database_exists():
    """설정된 데이터베이스가 존재하지 않을 경우 자동으로 생성합니다."""
    host, port, user, password, database = _get_pg_config()

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres",
            user=user,
            password=password,
            connect_timeout=10,
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f'CREATE DATABASE "{database}"')
            print(f"데이터베이스 '{database}'가 성공적으로 생성되었습니다.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"데이터베이스 존재 확인 중 오류 발생: {e}")
        raise


def get_db_connection(max_retries: int = 3):
    """데이터베이스 연결 객체를 반환합니다. 실패 시 재시도 로직을 포함합니다."""
    host, port, user, password, database = _get_pg_config()

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=10,
            )
            return conn
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e).lower():
                print(f"데이터베이스 '{database}'가 없습니다. 생성을 시도합니다...")
                ensure_database_exists()
                continue

            print(f"연결 시도 {attempt+1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise Exception(f"{max_retries}회 시도 후 데이터베이스 연결에 실패했습니다: {e}") from e


def init_db():
    """애플리케이션에 필요한 모든 테이블과 인덱스를 초기화합니다."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS artists (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                mbid TEXT,
                url TEXT,
                yt_channel_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        cursor.execute(
            """
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
            """
        )

        cursor.execute("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS youtube_views BIGINT DEFAULT 0;")
        cursor.execute("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spotify_streams BIGINT DEFAULT 0;")
        cursor.execute("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS soundcloud_plays BIGINT DEFAULT 0;")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS artist_growth_data (
                id SERIAL PRIMARY KEY, 
                artist_name TEXT, 
                song_name TEXT, 
                metric_type TEXT, 
                date DATE, 
                value BIGINT
            );
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_metrics_artist_date ON daily_metrics(artist_id, date DESC);"
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS interviews (
                id SERIAL PRIMARY KEY,
                candidate_name VARCHAR(255) NOT NULL,
                position VARCHAR(255) NOT NULL,
                transcript TEXT,
                gemini_analysis TEXT,
                openai_analysis TEXT,
                friendli_analysis TEXT,
                s3_video_url VARCHAR(500),
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        conn.commit()
        cursor.close()
        conn.close()
        print("데이터베이스 초기화 완료")
    except Exception as e:
        print(f"데이터베이스 초기화 실패: {e}")
        raise


def df_query(sql: str, params=()):
    """Pandas DataFrame 형태로 조회 결과를 반환하는 헬퍼 함수입니다."""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(sql.replace("?", "%s"), conn, params=params)
        return df
    finally:
        conn.close()


def exec_sql(sql: str, params=()):
    """데이터를 삽입, 수정, 삭제하는 명령어를 실행하는 헬퍼 함수입니다."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql.replace("?", "%s"), params)
        conn.commit()
        cursor.close()
    finally:
        conn.close()