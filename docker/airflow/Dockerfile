# =============================================================================
# Apache Airflow 2.10.4 — Custom image com providers PostgreSQL e Spark
# =============================================================================
FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
