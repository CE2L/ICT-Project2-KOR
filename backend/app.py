import streamlit as st

st.set_page_config(page_title="음악 통합 시스템",layout="wide")

import pandas as pd
import altair as alt
from db import init_db,df_query
from data_processing import process_and_upload_excel,delete_artist_and_data,get_lyrics_from_s3,pg_engine
from analytics import get_artists,get_artist_metrics_cached,plot_artist_growth_matplotlib,predict_milestone,plot_with_forecast,calculate_engagement_ratio,calculate_volatility_index,calculate_momentum_score
from services import get_lastfm_data,run_judge_panel,parse_ai_response,determine_grade_range,JUDGES


def ensure_extended_tables():
    try:
        from db import exec_sql
        exec_sql("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS youtube_views BIGINT DEFAULT 0;")
        exec_sql("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS spotify_streams BIGINT DEFAULT 0;")
        exec_sql("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS soundcloud_plays BIGINT DEFAULT 0;")
        exec_sql("CREATE TABLE IF NOT EXISTS artist_growth_data (id SERIAL PRIMARY KEY, artist_name TEXT, song_name TEXT, metric_type TEXT, date DATE, value BIGINT);")
    except Exception as e:
        st.error(f"테이블 생성 오류: {e}")


init_db()
ensure_extended_tables()


main_tab1,main_tab2,main_tab3=st.tabs(["아티스트 성장 레이더","AGT 음악 심사 AI","고급 분석 & 데이터 엔지니어링"])


with main_tab1:
    st.title("아티스트 성장 레이더 대시보드")
    with st.sidebar:
        st.header("관리")
        t2,t3=st.tabs(["동기화","삭제"])
        with t2:
            if st.button("동기화 실행"):
                with st.spinner("데이터 동기화 중..."):
                    try:
                        process_and_upload_excel()
                        st.success("동기화가 성공적으로 완료되었습니다")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"동기화 실패: {e}")
        with t3:
            arts=get_artists()
            if not arts.empty:
                target=st.selectbox("삭제할 아티스트 선택",arts["name"].tolist())
                if st.button("삭제 확정") and st.checkbox("이 작업은 되돌릴 수 없음을 이해했습니다"):
                    delete_artist_and_data(target)
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.warning("데이터베이스에 아티스트가 없습니다")
    
    with st.expander("디버그 정보"):
        try:
            artist_count=df_query("SELECT COUNT(*) as cnt FROM artists;")
            st.write(f"DB 아티스트 총 수: {artist_count.iloc[0]['cnt']}")
            
            growth_count=df_query("SELECT COUNT(*) as cnt FROM artist_growth_data;")
            st.write(f"성장 데이터 총 레코드 수: {growth_count.iloc[0]['cnt']}")
            
            metrics_count=df_query("SELECT COUNT(*) as cnt FROM daily_metrics;")
            st.write(f"일별 지표 총 레코드 수: {metrics_count.iloc[0]['cnt']}")
            
            import os
            from config import FOLDER_PATH
            if os.path.exists(FOLDER_PATH):
                csv_files=[f for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
                st.write(f"발견된 CSV 파일 수: {len(csv_files)}")
                st.write(csv_files[:10])
            else:
                st.error(f"폴더를 찾을 수 없습니다: {FOLDER_PATH}")
        except Exception as e:
            st.error(f"디버그 오류: {e}")
    
    days=st.sidebar.selectbox("분석 기간(일)",[7,30,90,180],index=1)
    artists=get_artists()
    if not artists.empty:
        sel=st.selectbox("아티스트 프로필 선택",artists["name"].tolist())
        a_id=int(artists.loc[artists["name"]==sel,"id"].iloc[0])
        res=get_artist_metrics_cached(a_id,days)
        if res:
            c1,c2,c3=st.columns(3)
            c1.metric("모멘텀(파이어)",f"{res['fire']:.2f}x")
            c2.metric("성장 가속도",f"{res['accel']:+.1f}%")
            c3.metric("추세 안정성",f"{res['stab']:.0f}/100")
            tab1,tab2=st.tabs(["원시 데이터 & 지표","시각화 트렌드"])
            with tab1:
                st.line_chart(res["df"].set_index("date")[res["active"]])
                st.dataframe(res["df"].iloc[::-1],use_container_width=True)
            with tab2:
                fig=plot_artist_growth_matplotlib(sel,pg_engine)
                if fig:
                    st.pyplot(fig)
        else:
            st.warning("100,000 기준을 넘는 일관된 데이터가 없습니다.")
    else:
        st.warning("아티스트가 없습니다. 먼저 동기화를 실행해주세요.")


with main_tab2:
    st.title("AGT AI 오디션")
    if st.button("글로벌 오디션 시작", type="primary"):
        songs = get_lyrics_from_s3()
        for song in songs:
            st.divider()
            st.subheader(f"{song['artist']} - {song['title']}")
            img_url, tags = get_lastfm_data(song["artist"], song["title"])

            img_col, s_col, h_col, m_col = st.columns([1, 3, 3, 3])

            if img_url:
                img_col.image(img_url, use_container_width=True)
            else:
                img_col.write("앨범 아트 없음")

            if tags:
                img_col.write(f"태그: {', '.join(tags)}")
            else:
                img_col.write("태그: 사용 불가")

            final_results = run_judge_panel(song, tags, img_col, s_col, h_col, m_col)

            total_score = sum(r["scores"]["Total"] for r in final_results.values())
            st.write(f"총점: {total_score} / 300 | 상태: {determine_grade_range(total_score)}")

            chart_data = [{"Judge": jn, "Score": final_results[jn]["scores"]["Total"]} for jn in JUDGES.keys()]
            st.altair_chart(
                alt.Chart(pd.DataFrame(chart_data))
                .mark_bar()
                .encode(
                    x=alt.X("Judge", sort=None),
                    y="Score",
                    color="Judge"
                ),
                use_container_width=True
            )



with main_tab3:
    st.title("고급 데이터 인사이트 & 엔지니어링")
    artists=get_artists()
    if not artists.empty:
        sel_adv=st.selectbox("고급 분석용 아티스트 선택",artists["name"].tolist(),key="adv_sel")
        a_id_adv=int(artists.loc[artists["name"]==sel_adv,"id"].iloc[0])
        metrics_adv=df_query("SELECT date,youtube_views,spotify_streams,soundcloud_plays FROM daily_metrics WHERE artist_id=%s",(a_id_adv,))
        metrics_adv["date"]=pd.to_datetime(metrics_adv["date"])
        col_a,col_b=st.columns(2)
        with col_a:
            st.subheader("예측 분석")
            yt_target=100000000
            milestone=predict_milestone(metrics_adv,"youtube_views",yt_target)
            st.metric("유튜브 1억 예상 날짜",str(milestone))
            forecast_fig=plot_with_forecast(metrics_adv,"spotify_streams")
            if forecast_fig:
                st.pyplot(forecast_fig)
            else:
                st.info("예측을 위한 100,000 기준 이상의 데이터가 충분하지 않습니다.")
        with col_b:
            st.subheader("플랫폼 상관관계")
            df_corr=metrics_adv.copy()
            for col in ["youtube_views","spotify_streams","soundcloud_plays"]:
                df_corr=df_corr[df_corr[col]>100000]
            if len(df_corr)>10:
                corr=df_corr[["youtube_views","spotify_streams","soundcloud_plays"]].corr()
                st.dataframe(corr.style.background_gradient(cmap="coolwarm"))
            else:
                st.info("상관관계 분석을 위한 100,000 기준 이상의 데이터가 충분하지 않습니다.")
    st.divider()
    st.subheader("확장 지표 통계")
    c1_stat,c2_stat,c3_stat=st.columns(3)
    with c1_stat:
        st.write("변동성 지수")
        for platform in ["youtube_views","spotify_streams","soundcloud_plays"]:
            vol=calculate_volatility_index(metrics_adv,platform)
            if vol is not None:
                st.metric(platform.replace("_"," ").title(),f"{vol}%")
            else:
                st.metric(platform.replace("_"," ").title(),"N/A")
    with c2_stat:
        st.write("7일 모멘텀 점수")
        for platform in ["youtube_views","spotify_streams","soundcloud_plays"]:
            mom=calculate_momentum_score(metrics_adv,platform)
            if mom is None:
                st.metric(platform.replace("_"," ").title(),"N/A")
            else:
                st.metric(platform.replace("_"," ").title(),f"{mom:.2f}x")
    with c3_stat:
        st.write("플랫폼 참여 분포")
        ratios=calculate_engagement_ratio(metrics_adv)
        if ratios:
            for platform,ratio in ratios.items():
                st.metric(platform,f"{ratio}%")
        else:
            st.info("100,000 기준 이상의 데이터가 충분하지 않습니다.")
    st.divider()
    st.subheader("데이터 엔지니어링: Airflow DAG 시뮬레이션")
    st.code(
        """
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG('chartmetric_daily_sync',schedule_interval='@daily',start_date=datetime(2026,1,1)) as dag:
    sync=PythonOperator(task_id='sync_s3',python_callable=process_and_upload_excel)
    analyze=PythonOperator(task_id='analyze_metrics',python_callable=calculate_momentum_score)
    sync>>analyze
""",
        language="python"
    )
