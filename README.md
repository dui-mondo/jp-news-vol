# jp-news-vol

Live pipeline that predicts next-day volatility spikes for large Tokyo-listed stocks, using price features and news-volume anomalies.

```
RSS (Google News, Yahoo Finance) --hourly--> Postgres raw_news --.
yfinance prices --weekdays after close--> Postgres raw_prices ----+--> dbt (features) --> model --> predictions --> Streamlit
```

- **Airflow**: `ingest_rss` (hourly), `daily_pipeline` (weekdays 16:00 JST: prices -> dbt run/test -> score past predictions -> train -> predict)
- **dbt**: `daily_news`, `features` (rolling z-scores, volatility ratios, spike label) with tests
- **Model**: HistGradientBoosting, time-based split, reports AUC of price-only vs price+news so you can see whether news adds anything
- **Scorecard**: every prediction is stored and later compared with the real outcome
- No cloud account needed. Everything runs in Docker.

## Run

```bash
docker compose up --build
```
- Airflow: http://localhost:8080 (admin / admin). Trigger `ingest_rss`, then `daily_pipeline`.
- Dashboard: http://localhost:8501

## Limitations (be upfront about these in interviews)

- RSS only returns recent items, so news history starts the day you start the pipeline. Before that, news features are zero, and the news lift in the AUC chart will be small until history accumulates.
- Target: next-day high-low range greater than 1.5x the trailing 20-day average. It is a proxy for a volatility spike, not a trading signal.
- Japanese market holidays are handled implicitly (no price row, no feature row).
