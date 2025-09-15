{{ config(
    materialized='table',
    pre_hook=[
        "SET s3_endpoint='localhost:9000'",
        "SET s3_use_ssl=false", 
        "SET s3_access_key_id='admin'",
        "SET s3_secret_access_key='password'",
        "SET s3_url_style='path'"
    ]
) }}


SELECT 
    vs.video_id,
    sr.keyword_id,
    k.keyword,
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
    sr.searched_at as keyword_searched_at
FROM {{ source('silver', 'silver_video_statistics') }} vs
JOIN {{ source('silver', 'silver_search_results') }} sr ON sr.video_id = vs.video_id
JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = sr.keyword_id
ORDER BY keyword_searched_at DESC
