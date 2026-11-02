import os

import psycopg2
from sqlalchemy import create_engine

DB_URL = os.environ.get("MARKET_DB_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/market")

# ticker -> names used to search news (first name is the search term)
TICKERS = {
    "7203.T": ["Toyota"],
    "6758.T": ["Sony"],
    "8306.T": ["Mitsubishi UFJ"],
    "9984.T": ["SoftBank Group"],
    "6861.T": ["Keyence"],
    "8035.T": ["Tokyo Electron"],
}


def conn():
    return psycopg2.connect(DB_URL.replace("+psycopg2", ""))


def engine():
    return create_engine(DB_URL)
