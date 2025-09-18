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




WITH video_time_series AS (
    SELECT 
        vs.video_id,
        k.keyword,
        sr.title,
        sr.channel_title,
        sr.channel_id,
        sr.publish_time,
        DATE(vs.retrieved_at) as performance_date,
        vs.view_count,
        vs.like_count,
        vs.comment_count,
        vs.duration_seconds,
        vs.view_count - LAG(vs.view_count) OVER (
            PARTITION BY vs.video_id 
            ORDER BY vs.retrieved_at
        ) as daily_view_growth,
        ROUND((vs.like_count * 100.0 / NULLIF(vs.view_count, 0)), 2) as like_rate,
        ROUND((vs.comment_count * 100.0 / NULLIF(vs.view_count, 0)), 2) as comment_rate,
        ROUND(((vs.like_count + vs.comment_count) * 100.0 / NULLIF(vs.view_count, 0)), 2) as engagement_rate
    FROM {{ source('silver', 'silver_video_statistics') }} vs
    JOIN {{ source('silver', 'silver_search_results') }} sr ON sr.video_id = vs.video_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = sr.keyword_id
)

SELECT 
    video_id,
    keyword,
    title,
    channel_title,
    channel_id,
    publish_time,
    performance_date,
    view_count,
    like_count,
    comment_count,
    duration_seconds,
    daily_view_growth,
    like_rate,
    comment_rate,
    engagement_rate
FROM video_time_series
ORDER BY performance_date DESC, view_count DESC