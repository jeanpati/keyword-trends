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



WITH trending_videos AS (
    SELECT 
        vs.video_id,
        k.keyword,
        sr.title,
        sr.channel_title,
        sr.publish_time,
        DATE(vs.retrieved_at) as performance_date,
        vs.view_count,
        vs.like_count,
        vs.comment_count,
        ROUND(((vs.like_count + vs.comment_count) * 100.0 / NULLIF(vs.view_count, 0)), 2) as engagement_rate,
        vs.view_count - LAG(vs.view_count) OVER (
            PARTITION BY vs.video_id 
            ORDER BY vs.retrieved_at
        ) as daily_view_growth,
        DATE_DIFF('day', CAST(sr.publish_time AS DATE), DATE(vs.retrieved_at)) as days_since_publish
    FROM {{ source('silver', 'silver_video_statistics') }} vs
    JOIN {{ source('silver', 'silver_search_results') }} sr ON vs.video_id = sr.video_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON sr.keyword_id = k.keyword_id
)

SELECT 
    video_id,
    keyword,
    title,
    channel_title,
    publish_time,
    performance_date,
    view_count,
    like_count,
    comment_count,
    engagement_rate,
    daily_view_growth,
    days_since_publish
FROM trending_videos
ORDER BY performance_date DESC, view_count DESC