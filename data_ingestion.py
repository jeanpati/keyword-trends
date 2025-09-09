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
from sqlalchemy import create_engine, text, Column, String, Text, DateTime
from datetime import datetime, timezone
from sqlalchemy.ext.declarative import declarative_base


logger = setup_logger(__name__, "./logs/data_ingestion.log")
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
CATALOG_PATH = os.getenv("CATALOG_PATH")
MINIO_FILE_PATH = os.getenv("MINIO_FILE_PATH")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

Base = declarative_base()


class SearchResult(Base):
    __tablename__ = "search_results"

    video_id = Column(String)
    search_keyword = Column(String)
    title = Column(String)
    description = Column(Text)
    channel_id = Column(String)
    channel_title = Column(String)
    publish_time = Column(DateTime)
    thumbnail_url = Column(String)
    searched_at = Column(DateTime)


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


def search_youtube_videos(keyword):
    """
    Retrieves keywords and saves search results for each keyword.
    """
    api_service_name = "youtube"
    api_version = "v3"
    max_results = 20

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=YOUTUBE_API_KEY
    )
    try:
        request = youtube.search().list(
            part="snippet",
            q=keyword,
            regionCode="US",
            maxResults=max_results,
            type="video",
            order="relevance",
        )

        response = request.execute()

        search_results = []
        for item in response.get("items", []):
            video_data = {
                "video_id": item["id"]["videoId"],
                "search_keyword": keyword,
                "title": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "channel_id": item["snippet"]["channelId"],
                "channel_title": item["snippet"]["channelTitle"],
                "publish_time": item["snippet"]["publishTime"],
                "searched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            search_results.append(video_data)

        logger.info("Found %s videos for keyword: %s", len(search_results), keyword)
        return search_results

    except Exception as e:
        logger.error("Error searching for keyword '%s': %s", keyword, e)
        return []


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
                    f"ATTACH 'ducklake:{CATALOG_PATH}' AS trends_lake (DATA_PATH '{MINIO_FILE_PATH}')"
                )
            )
        conn.execute(text("USE trends_lake"))
        conn.commit()

    return engine


def get_keywords():
    """
    Retrieve all keywords from the keywords table
    """
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


def save_search_results(search_results_data):
    """
    Save search results to DuckLake
    """
    if not search_results_data:
        logger.warning("No search results to save")
        return

    engine = get_sqlalchemy_engine()

    try:
        Base.metadata.create_all(engine, tables=[SearchResult.__table__])

        search_results_dataframe = pd.DataFrame(search_results_data)
        search_results_dataframe.to_sql(
            "search_results", engine, if_exists="append", index=False, method="multi"
        )

        logger.info(
            "Saved %s search results to database", len(search_results_dataframe)
        )

    except Exception as e:
        logger.error("Error saving search results: %s", e)


def main():
    convert_minio_csv_to_parquet()
    keywords = get_keywords()
    if not keywords:
        logger.error("No keywords found, exiting")
        return

    all_search_results = []
    for keyword in keywords:
        logger.info("Processing keyword: %s", keyword)
        search_results = search_youtube_videos(keyword)
        all_search_results.extend(search_results)
        time.sleep(1)

    save_search_results(all_search_results)


if __name__ == "__main__":
    main()
