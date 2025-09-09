import os
import requests
from dotenv import load_dotenv
from minio import Minio
from utils.logger import setup_logger
import time
import pandas as pd
from io import BytesIO
import urllib3
import googleapiclient.discovery
import googleapiclient.errors
from sqlalchemy import create_engine, text


logger = setup_logger(__name__, "./logs/data_ingestion.log")
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
CATALOG_PATH = os.getenv("CATALOG_PATH")
MINIO_FILE_PATH = os.getenv("MINIO_FILE_PATH")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def convert_minio_csv_to_parquet():
    """
    Converts the CSV files found in Minio to Parquet format.
    """
    start = time.perf_counter()
    http_client = urllib3.PoolManager(
        cert_reqs="CERT_NONE",
    )
    minio_client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
        http_client=http_client,
    )
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info("Created bucket: %s", MINIO_BUCKET)
    else:
        logger.info("Bucket %s already exists", MINIO_BUCKET)

    raw_folder = "csv/"
    converted_folder = "parquet/"
    raw_blobs = minio_client.list_objects(
        MINIO_BUCKET, prefix=raw_folder, recursive=True
    )

    for file in raw_blobs:
        response = minio_client.get_object(MINIO_BUCKET, file.object_name)
        logger.info("Processing file from MinIO: %s", file.object_name)

        if file.object_name == "csv/census.csv":
            csv_dataframe = pd.read_csv(response, skiprows=1)
        else:
            csv_dataframe = pd.read_csv(response)

        parquet_buffer = BytesIO()
        csv_dataframe.to_parquet(parquet_buffer, index=False)

        file_name = file.object_name.replace(raw_folder, converted_folder).replace(
            ".csv", ".parquet"
        )
        file_data = parquet_buffer.getvalue()
        file_size = len(file_data)
        file_data_buffer = BytesIO(file_data)

        minio_client.put_object(
            MINIO_BUCKET,
            file_name,
            file_data_buffer,
            file_size,
        )

        response.close()

    time_elapsed = round(time.perf_counter() - start, 2)
    logger.info(
        "Converted CSV files in Minio to Parquet, time_elapsed: %.2f seconds, %.2f minutes",
        time_elapsed,
        time_elapsed / 60,
    )


def save_search_data():
    """
    Retrieves keywords and saves search results for each keyword.
    """

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=YOUTUBE_API_KEY
    )

    request = youtube.search().list(
        part="snippet", q="vitamin c serum", regionCode="US", maxResults=20
    )
    response = request.execute()

    print(response)


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
        conn.execute(
            text(
                f"ATTACH 'ducklake:{CATALOG_PATH}' AS trends_lake (DATA_PATH '{MINIO_FILE_PATH}')"
            )
        )
        conn.execute(text("USE trends_lake"))
        conn.commit()

    return engine


def get_keywords():
    """Retrieve all keywords from the keywords table"""
    engine = get_sqlalchemy_engine()

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT DISTINCT keyword FROM keywords WHERE keyword IS NOT NULL")
            )
            keywords = [row[0] for row in result]

        logger.info("Retrieved %s keywords from database", len(keywords))
        return keywords

    except Exception as e:
        logger.error("Error retrieving keywords: %s", e)
        return []


def main():
    convert_minio_csv_to_parquet()
    keywords = get_keywords()
    print(keywords)


if __name__ == "__main__":
    main()
