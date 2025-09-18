import os
import pandas as pd
import googleapiclient.discovery
from sqlalchemy import text
from datetime import datetime, timezone
from dotenv import load_dotenv
from utils.sqlalchemy_engine import get_ducklake_engine
from utils.logger import setup_logger
from sqlalchemy import inspect


load_dotenv()
logger = setup_logger(__name__, "./logs/update_video_statistics.log")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def get_video_ids(limit=2000):
    """
    Retrieve video IDs that need statistics updates
    """
    engine = get_ducklake_engine()

    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            tables = inspector.get_table_names()
            if "video_statistics" in tables:
                # Get videos that don't have statistics yet
                result = conn.execute(
                    text(
                        """
                        SELECT DISTINCT sr.video_id 
                        FROM search_results sr
                        LEFT JOIN video_statistics vs ON sr.video_id = vs.video_id
                        WHERE sr.video_id IS NOT NULL AND vs.video_id IS NULL
                        LIMIT :limit
                        """
                    ),
                    {"limit": limit},
                )
            else:
                # Get all video IDs since statistics table doesn't exist
                result = conn.execute(
                    text(
                        "SELECT DISTINCT video_id FROM search_results WHERE video_id IS NOT NULL LIMIT :limit"
                    ),
                    {"limit": limit},
                )

            video_ids = [row[0] for row in result]

        logger.info("Retrieved %s video IDs for statistics update", len(video_ids))
        return video_ids

    except Exception as e:
        logger.error("Error retrieving video IDs: %s", e)
        raise


def get_video_statistics(video_ids):
    """
    Get detailed statistics for each video
    """
    if not video_ids:
        return []

    youtube = googleapiclient.discovery.build(
        "youtube", "v3", developerKey=YOUTUBE_API_KEY
    )

    try:
        video_stats = []
        # Process in batches of 50 (YouTube API limit)
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i : i + 50]

            request = youtube.videos().list(
                part="statistics,contentDetails", id=",".join(batch_ids)
            )
            response = request.execute()

            for item in response.get("items", []):
                video_data = {
                    "video_id": item.get("id", None),
                    "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
                    "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
                    "comment_count": int(
                        item.get("statistics", {}).get("commentCount", 0)
                    ),
                    "duration": item.get("contentDetails", {}).get("duration", None),
                    "retrieved_at": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
                video_stats.append(video_data)

        logger.info("Retrieved statistics for %s videos", len(video_stats))
        return video_stats

    except Exception as e:
        logger.error("Error getting video statistics: %s", e)
        raise


def save_video_statistics(video_stats_data):
    """Save video statistics to DuckLake"""
    if not video_stats_data:
        logger.warning("No video statistics to save")
        return

    engine = get_ducklake_engine()

    try:
        video_stats = pd.DataFrame(video_stats_data)
        video_stats = video_stats.drop_duplicates(subset=["video_id"], keep="first")

        video_stats.to_sql(
            "video_statistics", engine, if_exists="append", index=False, method="multi"
        )
        logger.info("Processed %d video statistics", len(video_stats))

    except Exception as e:
        logger.error("Error saving video statistics: %s", e)
        raise


def main():
    logger.info("Starting video statistics update")
    limit = 2000

    video_ids = get_video_ids(limit)
    if not video_ids:
        logger.info("No videos found for statistics update")
        return

    video_stats = get_video_statistics(video_ids)
    save_video_statistics(video_stats)

    logger.info(
        "Video statistics update completed. Processed %d videos", len(video_stats)
    )


if __name__ == "__main__":
    main()
