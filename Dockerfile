FROM apache/airflow:2.10.4-python3.11
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# dbt lives in its own venv so its dependencies can't clash with Airflow's
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.8.2
