{{ config(
    materialized='table',
    pre_hook=[
        "SET s3_endpoint='{{ env_var('MINIO_URL', 'localhost:9000') }}'",
        "SET s3_use_ssl=false", 
        "SET s3_access_key_id='admin'",
        "SET s3_secret_access_key='password'",
        "SET s3_url_style='path'"
    ]
) }}

WITH video_metrics_over_time AS (
    SELECT 
        vs.video_id,
        sr.keyword_id,
        k.keyword as search_keyword,
        sr.title,
        sr.description,
        sr.channel_id,
        sr.channel_title,
        vs.view_count,
        vs.like_count,
        vs.comment_count, 
        vs.duration,
        vs.duration_seconds,
        sr.publish_time,
        vs.retrieved_at as statistics_retrieved_at,
        sr.searched_at as keyword_searched_at,
        ROW_NUMBER() OVER (PARTITION BY vs.video_id ORDER BY vs.retrieved_at ASC) as first_measurement,
        ROW_NUMBER() OVER (PARTITION BY vs.video_id ORDER BY vs.retrieved_at DESC) as latest_measurement
    FROM {{ source('silver', 'silver_video_statistics') }} vs
    JOIN {{ source('silver', 'silver_search_results') }} sr ON sr.video_id = vs.video_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = sr.keyword_id

),

first_and_latest_stats AS (
    SELECT 
        video_id,
        search_keyword,
        title,
        channel_title,
        MAX(CASE WHEN first_measurement = 1 THEN view_count END) as initial_views,
        MAX(CASE WHEN first_measurement = 1 THEN like_count END) as initial_likes,
        MAX(CASE WHEN first_measurement = 1 THEN comment_count END) as initial_comments,
        MAX(CASE WHEN first_measurement = 1 THEN statistics_retrieved_at END) as first_retrieved_at,

        MAX(CASE WHEN latest_measurement = 1 THEN view_count END) as latest_views,
        MAX(CASE WHEN latest_measurement = 1 THEN like_count END) as latest_likes,
        MAX(CASE WHEN latest_measurement = 1 THEN comment_count END) as latest_comments,
        MAX(CASE WHEN latest_measurement = 1 THEN statistics_retrieved_at END) as latest_retrieved_at,

        COUNT(*) as total_measurements
    FROM video_metrics_over_time
    GROUP BY video_id, search_keyword, title, channel_title
    HAVING COUNT(*) > 1
),

keyword_growth_metrics AS (
    SELECT 
        search_keyword,
        COUNT(DISTINCT video_id) as videos_tracked,
        AVG(
            CASE 
                WHEN initial_views > 0 
                THEN ((latest_views - initial_views) * 100.0 / initial_views)
                ELSE 0 
            END
        ) as avg_view_growth_percent,
        AVG(
            CASE 
                WHEN initial_likes > 0 
                THEN ((latest_likes - initial_likes) * 100.0 / initial_likes)
                ELSE 0 
            END
        ) as avg_like_growth_percent,
        AVG(
            CASE 
                WHEN initial_comments > 0 
                THEN ((latest_comments - initial_comments) * 100.0 / initial_comments)
                ELSE 0 
            END
        ) as avg_comment_growth_percent,
        SUM(latest_views - initial_views) as total_view_growth,
        SUM(latest_likes - initial_likes) as total_like_growth,
        SUM(latest_comments - initial_comments) as total_comment_growth,
        AVG(DATE_DIFF('day', first_retrieved_at, latest_retrieved_at)) as avg_days_tracked
    FROM first_and_latest_stats
    GROUP BY search_keyword
)

SELECT 
    search_keyword,
    videos_tracked,
    ROUND(avg_view_growth_percent, 2) as avg_view_growth_percent,
    ROUND(avg_like_growth_percent, 2) as avg_like_growth_percent, 
    ROUND(avg_comment_growth_percent, 2) as avg_comment_growth_percent,
    total_view_growth,
    total_like_growth,
    total_comment_growth,
    ROUND(avg_days_tracked, 1) as avg_days_tracked,
FROM keyword_growth_metrics
WHERE videos_tracked >= 1
ORDER BY avg_view_growth_percent DESC