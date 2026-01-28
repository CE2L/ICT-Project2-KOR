import pandas as pd
import numpy as np
from datetime import date,timedelta
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import streamlit as st
from sklearn.linear_model import LinearRegression
from db import df_query


def set_font():
    font_list=[f.name for f in fm.fontManager.ttflist]
    preferred_fonts=["NanumGothic","Malgun Gothic","AppleGothic","Noto Sans KR"]
    for font in preferred_fonts:
        if font in font_list:
            plt.rcParams["font.family"]=font
            plt.rcParams["axes.unicode_minus"]=False
            return
    plt.rcParams["font.family"]="DejaVu Sans"
    plt.rcParams["axes.unicode_minus"]=False


def plot_artist_growth_matplotlib(artist_name,pg_engine):
    query=f"SELECT metric_type,date,SUM(value) as total_value FROM artist_growth_data WHERE artist_name='{artist_name}' AND date<CURRENT_DATE GROUP BY metric_type,date ORDER BY date ASC"
    df=pd.read_sql(query,pg_engine)
    if df.empty:
        return None
    active_platforms=df.groupby("metric_type")["total_value"].max()
    active_list=active_platforms[active_platforms>0].index.tolist()
    pivoted=df.pivot(index="date",columns="metric_type",values="total_value").fillna(0)
    for plat in active_list:
        pivoted=pivoted[pivoted[plat]>100000]
    if pivoted.empty:
        return None
    set_font()
    plt.style.use("ggplot")
    fig,axes=plt.subplots(len(active_list),1,figsize=(10,4*len(active_list)))
    if len(active_list)==1:
        axes=[axes]
    colors={"YouTube":"#FF0000","Spotify":"#1DB954","SoundCloud":"#FF5500"}
    for ax,m_name in zip(axes,active_list):
        ax.plot(pivoted.index,pivoted[m_name],color=colors.get(m_name,"#1DB954"),linewidth=2)
        ax.set_title(f"{m_name} Growth Trend",fontsize=12)
    plt.tight_layout()
    return fig


@st.cache_data(ttl=60)
def get_artists():
    return df_query("SELECT id,name FROM artists WHERE name!='TaeRyong' ORDER BY name;")


@st.cache_data(ttl=30)
def get_artist_metrics_cached(artist_id,days):
    end_date=date.today()-timedelta(days=1)
    start_date=end_date-timedelta(days=days)
    data=df_query(
        "SELECT date,youtube_views,spotify_streams,soundcloud_plays FROM daily_metrics WHERE artist_id=%s AND date>=%s AND date<=%s ORDER BY date ASC;",
        (artist_id,start_date.isoformat(),end_date.isoformat())
    )
    if data.empty:
        return None
    df=data.copy()
    df["date"]=pd.to_datetime(df["date"])
    cols=["youtube_views","spotify_streams","soundcloud_plays"]
    for c in cols:
        df[c]=pd.to_numeric(df[c],errors="coerce").fillna(0)
    active_cols=[c for c in cols if df[c].max()>0]
    if not active_cols:
        return None
    for c in active_cols:
        df=df[df[c]>100000]
    if len(df)<2:
        return None
    df["total"]=df[active_cols].sum(axis=1)
    df["growth"]=df["total"].diff().fillna(0)
    fire=df["growth"].tail(7).mean()/(df["growth"].mean()+1e-9)
    mid=len(df)//2
    accel=((df["growth"].tail(mid).mean()/(df["growth"].head(mid).mean()+1e-9))-1)*100
    stab=max(0,min(100,100-(df["growth"].std()/(df["growth"].mean()+1e-9)*20)))
    return {"df":df,"fire":fire,"accel":accel,"stab":stab,"active":active_cols}


def predict_milestone(df,column,target=100000000):
    if len(df)<5:
        return None
    df_sorted=df[df[column]>100000].sort_values("date")
    if len(df_sorted)<5:
        return None
    X=np.array((df_sorted["date"]-df_sorted["date"].min()).dt.days).reshape(-1,1)
    y=df_sorted[column].values
    model=LinearRegression().fit(X,y)
    if y[-1]>=target:
        return "Achieved"
    if model.coef_[0]<=0:
        return "Decreasing"
    days_to_target=(target-model.intercept_)/model.coef_[0]
    return df_sorted["date"].min()+timedelta(days=int(days_to_target))


def plot_with_forecast(df,column):
    if len(df)<5:
        return None
    df_filtered=df[df[column]>100000].sort_values("date")
    if len(df_filtered)<5:
        return None
    X=np.array((df_filtered["date"]-df_filtered["date"].min()).dt.days).reshape(-1,1)
    y=df_filtered[column].values
    model=LinearRegression().fit(X,y)
    future_days=np.array(range(int(X[-1][0]),int(X[-1][0])+31)).reshape(-1,1)
    future_preds=model.predict(future_days)
    future_dates=[df_filtered["date"].min()+timedelta(days=int(d)) for d in future_days.flatten()]
    fig,ax=plt.subplots(figsize=(10,4))
    ax.plot(df_filtered["date"],y,label="Actual",color="blue")
    ax.plot(future_dates,future_preds,label="Forecast",linestyle="--",color="orange")
    ax.set_title(f"{column} 30-Day Forecast")
    ax.legend()
    return fig


def calculate_engagement_ratio(df):
    cols=["youtube_views","spotify_streams","soundcloud_plays"]
    d=df[cols].copy()
    d=d.apply(pd.to_numeric,errors="coerce").fillna(0)
    d=d[d.sum(axis=1)>0]
    if d.empty:
        return None
    totals=d.sum()
    total_all=totals.sum()
    if total_all<=0:
        return None
    ratios={
        "YouTube Views":round(totals["youtube_views"]/total_all*100,1),
        "Spotify Streams":round(totals["spotify_streams"]/total_all*100,1),
        "SoundCloud Plays":round(totals["soundcloud_plays"]/total_all*100,1),
    }
    return ratios


def calculate_volatility_index(df,column,window=30):
    d=df[["date",column]].copy()
    d=d.sort_values("date")
    d[column]=pd.to_numeric(d[column],errors="coerce").fillna(0)
    d=d[d[column]>100000].copy()
    if len(d)<15:
        return None
    r=d[column].pct_change().replace([np.inf,-np.inf],np.nan).dropna()
    if len(r)<14:
        return None
    r=r.tail(window)
    vol=float(r.std()*100.0)
    vol=float(np.clip(vol,0.0,300.0))
    return round(vol,2)


def calculate_momentum_score(df,column,window=7):
    d=df[["date",column]].copy()
    d["date"]=pd.to_datetime(d["date"])
    d=d.sort_values("date")
    d[column]=pd.to_numeric(d[column],errors="coerce").fillna(0)
    d=d[d[column]>100000].copy()
    if len(d)<window*2+2:
        return None
    d["daily"]=d[column].diff()
    d=d.dropna()
    d=d[d["daily"]>0]
    if len(d)<window*2:
        return None
    recent_avg=float(d["daily"].tail(window).mean())
    prev_avg=float(d["daily"].iloc[-window*2:-window].mean())
    if prev_avg<=0:
        return None
    ratio=recent_avg/prev_avg
    ratio=float(np.clip(ratio,0.1,3.0))
    return round(ratio,2)
