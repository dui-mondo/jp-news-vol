FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit pandas sqlalchemy psycopg2-binary
COPY dashboard.py .
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
