import os
import re
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


def extract_keyword_and_dates(header_line):
    """
    Parse the header to get keyword, begin_date, and end_date
    """
    try:
        if ":" not in header_line or not header_line.startswith("Region"):
            return None, None, None

        keyword_part = header_line[7:].strip()  # removes "Region "
        keyword = keyword_part.split(":")[0].strip().lower()

        date_match = re.search(r"\(([^)]+)\)", header_line)
        if not date_match:
            return keyword, None, None

        date_range = date_match.group(1)
        begin_date, end_date = parse_date_range(date_range)

        return keyword, begin_date, end_date

    except Exception as e:
        logger.error("Error parsing header: %s", e)
        return None, None, None


def parse_date_range(date_range):
    """Parse date range string into begin_date and end_date"""
    try:
        if " - " not in date_range:
            return None, None

        start_str, end_str = date_range.split(" - ")

        date_format = "%m/%d/%y"

        begin_date = None
        end_date = None

        start_str = start_str.strip()

        begin_date = datetime.strptime(start_str, date_format).date()

        if not begin_date:
            logger.warning("Could not parse start date: %s", start_str)

        end_str = end_str.strip()

        end_date = datetime.strptime(end_str, date_format).date()

        if not end_date:
            logger.warning("Could not parse end date: %s", end_str)

        return begin_date, end_date

    except Exception as e:
        logger.error("Error parsing date range '%s': %s", date_range, e)
        return None, None


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
    for file in trends_blobs:
        file_path = file.object_name
        try:
            response = minio_client.get_object(MINIO_BUCKET, file_path)
            content = response.read().decode("utf-8")
            response.close()

            lines = content.strip().split("\n")
            keyword = ""
            begin_date = ""
            end_date = ""
            for line in lines:
                if (
                    "(" in line
                    and ")" in line
                    and " - " in line
                    and line.startswith("Region")
                ):
                    keyword, begin_date, end_date = extract_keyword_and_dates(line)
                    break

            if not keyword:
                logger.warning("No keyword found in: %s", file_path)
                continue

            logger.info("Processing '%s' (%s to %s)", keyword, begin_date, end_date)
        except Exception as e:
            logger.error("Error processing %s: %s", file, e)
            raise


def main():
    """
    Process all Google Trends files in Minio and save to database
    """

    logger.info("Processing files from Minio")
    process_trends_files()


if __name__ == "__main__":
    main()
