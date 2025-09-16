import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__, "./logs/gold_to_postgres.log")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/ducklake-db/lake.duckdb")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PORT = os.getenv("PG_PORT", 5432)

DUCKDB_GOLD_SCHEMA = os.getenv("DUCKDB_GOLD_SCHEMA")

export_tables = [
    "gold_demographics",
    "gold_dominant_videos",
    "gold_top_regions",
    "gold_trending_keywords",
    "gold_videos",
]

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@localhost:{PG_PORT}/{PG_DATABASE}"
)


with duckdb.connect(DUCKDB_PATH) as duck, engine.begin() as conn:
    for table in export_tables:
        export_table_data = duck.execute(
            f"SELECT * FROM {DUCKDB_GOLD_SCHEMA}.{table}"
        ).fetchdf()

        export_table_data.to_sql(
            table, conn, if_exists="append", index=False, method="multi"
        )

        logger.info("Uploaded %s rows to Postgres", len(export_table_data))
