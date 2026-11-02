import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="JP news -> volatility", layout="wide")
eng = create_engine(os.environ["MARKET_DB_URL"])


@st.cache_data(ttl=300)
def q(sql):
    return pd.read_sql(sql, eng)


st.title("Tokyo tickers: news anomalies and next-day volatility spikes")

try:
    preds = q("SELECT ticker, pred_date, prob_spike, actual_spike FROM predictions ORDER BY pred_date DESC, prob_spike DESC LIMIT 200")
    metrics = q("SELECT * FROM model_metrics ORDER BY run_at")
    anomalies = q("SELECT ticker, d, news_cnt, news_z, news_sent FROM features WHERE news_z > 2 ORDER BY d DESC LIMIT 30")
except Exception:
    st.info("No data yet. Run ingest_rss and daily_pipeline in Airflow (localhost:8080).")
    st.stop()

if preds.empty:
    st.info("No predictions yet. Trigger daily_pipeline in Airflow.")
    st.stop()

latest = preds[preds.pred_date == preds.pred_date.max()]
st.subheader(f"Latest predictions (data through {latest.pred_date.iloc[0]})")
st.dataframe(latest[["ticker", "prob_spike"]], hide_index=True)

scored = preds.dropna(subset=["actual_spike"])
c1, c2 = st.columns(2)
c1.metric("Scored predictions", len(scored))
if len(scored):
    hit = ((scored.prob_spike >= 0.5) == (scored.actual_spike == 1)).mean()
    c2.metric("Live hit rate (threshold 0.5)", f"{hit:.0%}")
    st.dataframe(scored.head(30), hide_index=True)

st.subheader("Does news help? Holdout AUC per training run")
if not metrics.empty:
    st.line_chart(metrics.set_index("run_at")[["auc_price_only", "auc_with_news"]])

st.subheader("Recent news-volume anomalies (z > 2)")
st.dataframe(anomalies, hide_index=True)
