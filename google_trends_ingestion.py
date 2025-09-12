import os
from datetime import datetime
import pandas as pd
from utils.logger import setup_logger
from dotenv import load_dotenv
from data_ingestion import get_sqlalchemy_engine, connect_to_minio

logger = setup_logger(__name__, "./logs/google_trends_ingestion.log")
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_FILE_PATH = os.getenv("MINIO_FILE_PATH")


def process_trends_files():
    """
    Process all Google Trends files in a directory and return combined DataFrame
    """
    minio_client = connect_to_minio()
    trends_folder = "csv/google_trends"

    trends_blobs = minio_client.list_objects(
        MINIO_BUCKET, prefix=trends_folder, recursive=True
    )

    trends_data = []
    for filename in trends_blobs:
        file_path = os.path.join(trends_folder, filename)
        print(file_path)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            lines = content.strip().split("\n")
            print(lines)
        except Exception as e:
            print("Error processing %s: %s", filename, e)
            raise


def main():
    """
    Process all Google Trends files in Minio and save to database
    """

    print("Processing files from Minio")
    process_trends_files()


if __name__ == "__main__":
    main()
