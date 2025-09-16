FROM apache/airflow:2.7.3-python3.11

USER root

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pandas requests duckdb dbt-core dbt-duckdb