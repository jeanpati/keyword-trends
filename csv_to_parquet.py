import os
from dotenv import load_dotenv
from minio import Minio
from utils.logger import setup_logger
import time
import pandas as pd
from io import BytesIO
import urllib3

logger = setup_logger(__name__, "./logs/csv_to_parquet.log")
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
CATALOG_PATH = os.getenv("CATALOG_PATH")
MINIO_PARQUET_PATH = os.getenv("MINIO_PARQUET_PATH")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def connect_to_minio():
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
    return minio_client


def convert_minio_csv_to_parquet():
    """
    Converts the CSV files found in Minio to Parquet format.
    """
    start = time.perf_counter()
    minio_client = connect_to_minio()

    raw_folder = "csv/"
    converted_folder = "parquet/"
    raw_blobs = minio_client.list_objects(
        MINIO_BUCKET, prefix=raw_folder, recursive=True
    )

    for file in raw_blobs:
        response = minio_client.get_object(MINIO_BUCKET, file.object_name)
        logger.info("Processing file from MinIO: %s", file.object_name)

        if file.object_name == "csv/census.csv":
            csv = pd.read_csv(response, skiprows=1)
        else:
            csv = pd.read_csv(response)

        parquet_buffer = BytesIO()
        csv.to_parquet(parquet_buffer, index=False)

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


def main():
    logger.info("Starting conversion")
    convert_minio_csv_to_parquet()

    logger.info("Converted CSV files to Parquet")


if __name__ == "__main__":
    main()
