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


WITH latest_searches AS (
    SELECT 
        keyword_id,
        MAX(searched_at) AS last_appeared_in_search
    FROM {{ source('silver', 'silver_search_results') }}
    GROUP BY keyword_id
),

top_20_per_keyword AS (
    SELECT 
        sr.*,
        ROW_NUMBER() OVER (
            PARTITION BY sr.keyword_id, sr.searched_at
            ORDER BY sr.searched_at DESC, sr.video_id
        ) as rn
    FROM {{ source('silver', 'silver_search_results') }} sr
    JOIN latest_searches ls ON sr.keyword_id = ls.keyword_id AND sr.searched_at = ls.last_appeared_in_search
),

channel_video_stats AS (
    SELECT 
        sr.channel_id,
        sr.channel_title,
        sr.video_id,
        k.keyword,
        vs.view_count,
        vs.like_count,
        vs.comment_count,
        vs.duration_seconds,
        ROUND((vs.like_count * 100.0 / NULLIF(vs.view_count, 0)), 2) as like_rate,
        ROUND((vs.comment_count * 100.0 / NULLIF(vs.view_count, 0)), 2) as comment_rate,
        ROUND(((vs.like_count + vs.comment_count) * 100.0 / NULLIF(vs.view_count, 0)), 2) as engagement_rate
    FROM top_20_per_keyword sr
    JOIN {{ source('silver', 'silver_video_statistics') }} vs ON sr.video_id = vs.video_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = sr.keyword_id
    WHERE sr.rn <= 20
),

channel_aggregates AS (
    SELECT 
        channel_id,
        channel_title,
        COUNT(DISTINCT video_id) as total_videos_tracked,
        COUNT(DISTINCT keyword) as keywords_appeared_in,
        SUM(view_count) as total_channel_views,
        AVG(view_count) as avg_views_per_video,
        MAX(view_count) as best_performing_video_views,
        MIN(view_count) as lowest_performing_video_views,
        AVG(like_rate) as avg_channel_like_rate,
        AVG(comment_rate) as avg_channel_comment_rate,
        AVG(engagement_rate) as avg_channel_engagement_rate,
        AVG(duration_seconds) as avg_video_duration_seconds
    FROM channel_video_stats
    GROUP BY channel_id, channel_title
)

SELECT 
    channel_id,
    channel_title,
    total_videos_tracked,
    keywords_appeared_in,
    total_channel_views,
    avg_views_per_video,
    best_performing_video_views,
    lowest_performing_video_views,
    avg_channel_like_rate,
    avg_channel_comment_rate,
    avg_channel_engagement_rate,
    avg_video_duration_seconds,
    ROUND(avg_video_duration_seconds / 60, 1) as avg_video_duration_minutes
FROM channel_aggregates
WHERE total_videos_tracked >= 2
ORDER BY total_channel_views DESC