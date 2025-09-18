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

latest_video_stats AS (
    SELECT 
        vs.video_id,
        sr.channel_id,
        sr.channel_title,
        k.keyword_id,
        k.keyword,
        vs.view_count,
        vs.like_count,
        vs.comment_count,
        vs.duration_seconds,
        ROUND(((vs.like_count + vs.comment_count) * 100.0 / NULLIF(vs.view_count, 0)), 2) as engagement_rate
    FROM {{ source('silver', 'silver_video_statistics') }} vs
    JOIN top_20_per_keyword sr ON vs.video_id = sr.video_id AND sr.rn <= 20
    JOIN {{ source('silver', 'silver_keywords') }} k ON sr.keyword_id = k.keyword_id
),

ecosystem_metrics AS (
    SELECT 
        COUNT(DISTINCT video_id) as total_videos_monitored,
        COUNT(DISTINCT keyword_id) as total_keywords_tracked,
        COUNT(DISTINCT channel_id) as total_channels_monitored,
        SUM(view_count) as total_ecosystem_views,
        AVG(view_count) as avg_video_views,
        MAX(view_count) as highest_video_views,
        AVG(engagement_rate) as avg_engagement_rate,
        MAX(engagement_rate) as max_engagement_rate,
        AVG(duration_seconds) as avg_video_duration_seconds
    FROM latest_video_stats
)

SELECT 
    CURRENT_DATE as report_date,
    CURRENT_TIMESTAMP as generated_at,
    em.total_videos_monitored,
    em.total_keywords_tracked,
    em.total_channels_monitored,
    em.total_ecosystem_views,
    ROUND(em.avg_video_views, 0) as avg_video_views,
    em.highest_video_views,
    ROUND(em.avg_engagement_rate, 2) as avg_engagement_rate,
    ROUND(em.max_engagement_rate, 2) as max_engagement_rate,
    ROUND(em.avg_video_duration_seconds / 60, 1) as avg_video_duration_minutes
FROM ecosystem_metrics em