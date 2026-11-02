CREATE DATABASE market;
\c market

CREATE TABLE raw_news (
  ticker       text        NOT NULL,
  url_hash     text        NOT NULL,
  published_at timestamptz NOT NULL,
  title        text,
  source       text,
  sentiment    real,
  fetched_at   timestamptz DEFAULT now(),
  PRIMARY KEY (ticker, url_hash)
);

CREATE TABLE raw_prices (
  ticker text NOT NULL,
  d      date NOT NULL,
  open   double precision, high double precision, low double precision,
  close  double precision, volume bigint,
  PRIMARY KEY (ticker, d)
);

CREATE TABLE predictions (
  ticker      text NOT NULL,
  pred_date   date NOT NULL,          -- last day of data the model saw
  prob_spike  real,                   -- P(next trading day range > 1.5x its 20d avg)
  actual_spike int,                   -- filled in once the next day exists
  created_at  timestamptz DEFAULT now(),
  PRIMARY KEY (ticker, pred_date)
);

CREATE TABLE model_metrics (
  run_at         timestamptz DEFAULT now(),
  n_train        int,
  n_test         int,
  auc_price_only real,
  auc_with_news  real
);
