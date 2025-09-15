import os
from dotenv import load_dotenv
from utils.logger import setup_logger
import pandas as pd
import googleapiclient.discovery
from sqlalchemy import text
from datetime import datetime, timezone
from utils.sqlalchemy_engine import get_sqlalchemy_engine

logger = setup_logger(__name__, "./logs/update_video_statistics.log")
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def get_video_ids_for_stats_update(limit=2000):
    """
    Retrieve video IDs that need statistics updates
    Prioritizes videos that haven't been updated recently
    """
    engine = get_sqlalchemy_engine()

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT DISTINCT sr.video_id 
                    FROM search_results sr
                    LEFT JOIN video_statistics vs ON sr.video_id = vs.video_id
                    WHERE sr.video_id IS NOT NULL
                    ORDER BY vs.retrieved_at ASC NULLS FIRST
                    LIMIT :limit
                """
                ),
                {"limit": limit},
            )

            video_ids = [row[0] for row in result]

        logger.info(
            "Retrieved %s video IDs for statistics update (limited to %s)",
            len(video_ids),
            limit,
        )
        return video_ids

    except Exception as e:
        logger.error("Error retrieving video IDs: %s", e)
        raise


def get_video_statistics(video_ids):
    """
    Get detailed statistics for each video - likes, views, and comments
    """
    if not video_ids:
        return []

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=YOUTUBE_API_KEY
    )

    try:
        video_stats = []
        # YouTube API allows up to 50 video IDs per request
        # Process in batches of 50
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i : i + 50]

            request = youtube.videos().list(
                part="statistics,contentDetails", id=",".join(batch_ids)
            )
            response = request.execute()

            for item in response.get("items", []):
                video_stats.append(
                    {
                        "video_id": item.get("id", None),
                        "view_count": int(
                            item.get("statistics", {}).get("viewCount", 0)
                        ),
                        "like_count": int(
                            item.get("statistics", {}).get("likeCount", 0)
                        ),
                        "comment_count": int(
                            item.get("statistics", {}).get("commentCount", 0)
                        ),
                        "duration": item.get("contentDetails", {}).get(
                            "duration", None
                        ),
                        "retrieved_at": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                )

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

    engine = get_sqlalchemy_engine()

    try:
        video_stats = pd.DataFrame(video_stats_data)
        video_stats = video_stats.drop_duplicates(
            subset=["video_id", "retrieved_at"], keep="first"
        )

        video_stats.to_sql(
            "video_statistics", engine, if_exists="append", index=False, method="multi"
        )
        logger.info(
            "Processed %d video statistics",
            len(video_stats),
        )

    except Exception as e:
        logger.error("Error saving video statistics: %s", e)
        raise


def main():
    logger.info("Starting video statistics update...")
    limit = 2000
    video_ids = get_video_ids_for_stats_update(limit)
    if not video_ids:
        logger.info("No videos found for statistics update")
        return

    logger.info("Getting statistics for %s videos", len(video_ids))
    video_stats = get_video_statistics(video_ids)
    save_video_statistics(video_stats)

    logger.info("Video statistics update completed.")


if __name__ == "__main__":
    main()
