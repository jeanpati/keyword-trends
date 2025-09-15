import os
import time
import pandas as pd
import googleapiclient.discovery
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv
from utils.sqlalchemy_engine import get_sqlalchemy_engine
from utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__, "./logs/youtube_search.log")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def get_keywords(limit):
    """
    Retrieve all keywords from the keywords table
    """
    engine = get_sqlalchemy_engine()

    try:
        with engine.connect() as conn:
            if limit:
                result = conn.execute(
                    text(
                        "SELECT DISTINCT keyword FROM keywords WHERE keyword IS NOT NULL LIMIT :limit"
                    ),
                    {"limit": limit},
                )
            else:
                result = conn.execute(
                    text(
                        "SELECT DISTINCT keyword FROM keywords WHERE keyword IS NOT NULL"
                    )
                )
            keywords = [row[0] for row in result]

        logger.info("Retrieved %s keywords from database", len(keywords))
        return keywords

    except Exception as e:
        logger.error("Error retrieving keywords: %s", e)
        raise


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
                "video_id": item.get("id", {}).get("videoId", None),
                "search_keyword": keyword,
                "title": item.get("snippet", {}).get("title", None),
                "description": item.get("snippet", {}).get("description", None),
                "channel_id": item.get("snippet", {}).get("channelId", None),
                "channel_title": item.get("snippet", {}).get("channelTitle", None),
                "publish_time": item.get("snippet", {}).get("publishTime", None),
                "searched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            }
            search_results.append(video_data)

        logger.info("Found %s videos for keyword: %s", len(search_results), keyword)
        return search_results

    except Exception as e:
        logger.error("Error searching for keyword '%s': %s", keyword, e)
        raise


def save_search_results(search_results_data):
    """Save search results to DuckLake"""
    if not search_results_data:
        logger.warning("No search results to save")
        return

    engine = get_sqlalchemy_engine()

    try:
        search_results = pd.DataFrame(search_results_data)
        search_results = search_results.drop_duplicates(
            subset=["video_id"], keep="first"
        )

        search_results.to_sql(
            "search_results", engine, if_exists="append", index=False, method=None
        )
        logger.info(
            "Processed %d search results",
            len(search_results),
        )

    except Exception as e:
        logger.error("Error saving search results: %s", e)
        raise


def main():
    logger.info("Starting YouTube search")
    keyword_limit = 50
    keywords = get_keywords(keyword_limit)
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
    logger.info(
        "YouTube search completed. Processed %d keywords, found %d videos",
        len(keywords),
        len(all_search_results),
    )


if __name__ == "__main__":
    main()
