"""Hourly: pull news headlines per ticker from RSS, score sentiment, upsert into raw_news."""
import calendar
import hashlib
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import feedparser
from airflow.decorators import dag, task
from psycopg2.extras import execute_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from common import TICKERS, conn


def feed_urls(ticker, names):
    q = quote(f'"{names[0]}" when:2d')
    return [
        f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en",
        f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
    ]


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["news"],
)
def ingest_rss():
    @task
    def fetch_and_store():
        sia = SentimentIntensityAnalyzer()
        rows = {}
        for ticker, names in TICKERS.items():
            for url in feed_urls(ticker, names):
                feed = feedparser.parse(url, agent="Mozilla/5.0 (jp-news-vol portfolio project)")
                for e in feed.entries:
                    link = e.get("link")
                    if not link or not e.get("published_parsed"):
                        continue
                    pub = datetime.fromtimestamp(calendar.timegm(e.published_parsed), tz=timezone.utc)
                    title = e.get("title", "").strip()
                    h = hashlib.md5(link.encode()).hexdigest()
                    rows[(ticker, h)] = (
                        ticker, h, pub, title, feed.feed.get("title", ""),
                        sia.polarity_scores(title)["compound"],
                    )
                time.sleep(2)  # be polite, avoid throttling
        if rows:
            with conn() as c, c.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO raw_news (ticker,url_hash,published_at,title,source,sentiment) "
                    "VALUES %s ON CONFLICT DO NOTHING",
                    list(rows.values()),
                )
        print(f"fetched {len(rows)} items")

    fetch_and_store()


ingest_rss()
