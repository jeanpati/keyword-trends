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

WITH search_frequency AS (
    SELECT 
        sr.keyword_id,
        k.keyword,
        sr.video_id,
        sr.title,
        sr.channel_title,
        sr.channel_id,
        COUNT(DISTINCT DATE(sr.searched_at)) as search_appearances,
        MIN(sr.searched_at) as first_appeared,
        MAX(sr.searched_at) as last_appeared,
        MAX(vs.view_count) as latest_view_count,
        MAX(vs.like_count) as latest_like_count,
        MAX(vs.comment_count) as latest_comment_count
    FROM {{ source('silver', 'silver_search_results') }} sr
    LEFT JOIN {{ source('silver', 'silver_video_statistics') }} vs ON vs.video_id = sr.video_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = sr.keyword_id
    GROUP BY sr.keyword_id, k.keyword, sr.video_id, sr.title, sr.channel_title, sr.channel_id
),

keyword_search_totals AS (
    SELECT 
        keyword,
        COUNT(DISTINCT DATE(searched_at)) as total_search_dates
    FROM {{ source('silver', 'silver_search_results') }}
    GROUP BY keyword
),

dominant_videos AS (
    SELECT 
        sf.*,
        kst.total_search_dates,
        ROUND((sf.search_appearances * 100.0 / kst.total_search_dates), 1) as dominance_percentage,
        DATE_DIFF('day', sf.first_appeared, sf.last_appeared) as days_stayed_relevant
    FROM search_frequency sf
    JOIN keyword_search_totals kst ON kst.keyword = sf.keyword
)

SELECT 
    keyword_id,
    keyword,
    video_id,
    title,
    channel_title,
    channel_id,
    search_appearances,
    total_search_dates,
    dominance_percentage,
    days_stayed_relevant,
    latest_view_count,
    latest_like_count,
    latest_comment_count,
    first_appeared,
    last_appeared,
    CASE 
        WHEN dominance_percentage >= 75 THEN 'Highly Dominant'
        WHEN dominance_percentage >= 50 THEN 'Moderately Dominant' 
        WHEN dominance_percentage >= 25 THEN 'Occasionally Dominant'
        ELSE 'Rare Appearance'
    END as dominance_category
FROM dominant_videos
WHERE search_appearances > 1
ORDER BY keyword ASC