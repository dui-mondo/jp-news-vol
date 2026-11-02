"""Weekdays after the Tokyo close: prices -> dbt features -> score old predictions -> train -> predict."""
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from psycopg2.extras import execute_values
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from common import TICKERS, conn, engine

PRICE_FEATS = ["ret", "range_pct", "vol5", "vol20", "vol_ratio", "range_rel"]
NEWS_FEATS = ["news_cnt", "news_sent", "news_z"]
DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"


def fit(df, feats):
    m = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.05, max_iter=200)
    return m.fit(df[feats], df["target_spike"].astype(int))


def auc(train, test, feats):
    try:
        m = fit(train, feats)
        return float(roc_auc_score(test["target_spike"].astype(int), m.predict_proba(test[feats])[:, 1]))
    except ValueError:  # test window had a single class
        return None


@task
def fetch_prices():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw_prices")
        first_run = cur.fetchone()[0] == 0
    period = "3y" if first_run else "10d"
    rows = []
    for t in TICKERS:
        h = yf.Ticker(t).history(period=period, auto_adjust=False)
        for d, r in h.iterrows():
            if r.Close != r.Close:  # NaN
                continue
            rows.append((t, d.date(), float(r.Open), float(r.High), float(r.Low), float(r.Close), int(r.Volume)))
    with conn() as c, c.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO raw_prices (ticker,d,open,high,low,close,volume) VALUES %s "
            "ON CONFLICT (ticker,d) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, "
            "low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume",
            rows,
        )
    print(f"upserted {len(rows)} price rows")


@task
def score_train_predict():
    # 1) score earlier predictions whose outcome is now known
    with conn() as c, c.cursor() as cur:
        cur.execute(
            "UPDATE predictions p SET actual_spike = f.target_spike FROM features f "
            "WHERE f.ticker=p.ticker AND f.d=p.pred_date "
            "AND f.target_spike IS NOT NULL AND p.actual_spike IS NULL"
        )

    # 2) load features, time-based split (no random shuffling)
    df = pd.read_sql("SELECT * FROM features ORDER BY d", engine())
    df["d"] = pd.to_datetime(df["d"])
    df[NEWS_FEATS] = df[NEWS_FEATS].astype(float).fillna(0)
    df = df.dropna(subset=PRICE_FEATS)
    lab = df[df["target_spike"].notna()]
    cut = lab["d"].sort_values().iloc[int(len(lab) * 0.8)]
    train = lab[lab["d"] < cut - pd.Timedelta(days=5)]  # gap avoids label overlap
    test = lab[lab["d"] >= cut]
    a_price = auc(train, test, PRICE_FEATS)
    a_all = auc(train, test, PRICE_FEATS + NEWS_FEATS)

    # 3) refit on everything labelled, predict the latest unlabelled day per ticker
    model = fit(lab, PRICE_FEATS + NEWS_FEATS)
    latest = df.sort_values("d").groupby("ticker").tail(1)
    latest = latest[latest["target_spike"].isna()]
    probs = model.predict_proba(latest[PRICE_FEATS + NEWS_FEATS])[:, 1]

    with conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO model_metrics (n_train,n_test,auc_price_only,auc_with_news) VALUES (%s,%s,%s,%s)",
            (len(train), len(test), a_price, a_all),
        )
        execute_values(
            cur,
            "INSERT INTO predictions (ticker,pred_date,prob_spike) VALUES %s "
            "ON CONFLICT (ticker,pred_date) DO UPDATE SET prob_spike=EXCLUDED.prob_spike",
            [(r.ticker, r.d.date(), float(p)) for r, p in zip(latest.itertuples(), probs)],
        )
    print(f"AUC price-only={a_price} with-news={a_all}; predicted {len(latest)} tickers")


@dag(
    schedule="0 7 * * 1-5",  # 16:00 JST, after the 15:30 close
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "retry_exponential_backoff": True},
    tags=["model"],
)
def daily_pipeline():
    dbt = BashOperator(task_id="dbt_run_and_test", bash_command=f"{DBT} run {DBT_ARGS} && {DBT} test {DBT_ARGS}")
    fetch_prices() >> dbt >> score_train_predict()


daily_pipeline()
