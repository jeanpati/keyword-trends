import os
from sqlalchemy import create_engine, text

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
CATALOG_PATH = os.getenv("CATALOG_PATH")
MINIO_PARQUET_PATH = os.getenv("MINIO_PARQUET_PATH")


def get_sqlalchemy_engine():
    """
    Create SQLAlchemy engine for DuckDB with DuckLake
    """
    db_path = "data/ducklake-db/lake.duckdb"
    engine = create_engine(f"duckdb:///{db_path}")

    with engine.connect() as conn:
        conn.execute(text("LOAD 'ducklake'"))
        conn.execute(text(f"SET s3_endpoint='{MINIO_URL}'"))
        conn.execute(text("SET s3_use_ssl=false"))
        conn.execute(text(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}'"))
        conn.execute(text(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}'"))
        conn.execute(text("SET s3_url_style='path'"))

        result = conn.execute(text("SHOW DATABASES")).fetchall()
        attached_databases = [row[0] for row in result]
        if "trends_lake" not in attached_databases:
            conn.execute(
                text(
                    f"ATTACH 'ducklake:{CATALOG_PATH}' AS trends_lake (DATA_PATH '{MINIO_PARQUET_PATH}')"
                )
            )
        conn.execute(text("USE trends_lake"))
        conn.commit()

    return engine
