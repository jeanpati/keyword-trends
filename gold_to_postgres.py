import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__, "./logs/gold_to_postgres.log")

DUCKDB_PATH = os.getenv("DUCKDB_PATH")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PORT = os.getenv("PG_PORT")
PG_HOST = os.getenv("PG_HOST")

DUCKDB_GOLD_SCHEMA = os.getenv("DUCKDB_GOLD_SCHEMA")

export_tables = [
    "gold_business_kpis",
    "gold_channel_insights",
    "gold_geographic_insights",
    "gold_keyword_market_analysis",
    "gold_trending_analysis",
    "gold_video_performance_time_series",
]

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)


def main():
    with duckdb.connect(DUCKDB_PATH) as duck, engine.begin() as conn:
        for table in export_tables:
            export_table_data = duck.execute(
                f"SELECT * FROM {DUCKDB_GOLD_SCHEMA}.{table}"
            ).fetchdf()

            export_table_data.to_sql(
                table, conn, if_exists="replace", index=False, method="multi"
            )

            logger.info("Uploaded %s rows to Postgres", len(export_table_data))


if __name__ == "__main__":
    main()
