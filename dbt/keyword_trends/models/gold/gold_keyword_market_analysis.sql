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

keyword_video_performance AS (
    SELECT 
        k.keyword_id,
        k.keyword,
        sr.video_id,
        sr.channel_id,
        sr.channel_title,
        vs.view_count,
        vs.like_count,
        vs.comment_count,
        ROUND(((vs.like_count + vs.comment_count) * 100.0 / NULLIF(vs.view_count, 0)), 2) as engagement_rate
    FROM {{ source('silver', 'silver_keywords') }} k
    JOIN top_20_per_keyword sr ON k.keyword_id = sr.keyword_id AND sr.rn <= 20
    JOIN {{ source('silver', 'silver_video_statistics') }} vs ON sr.video_id = vs.video_id
),

keyword_trends_data AS (
    SELECT 
        gt.keyword_id,
        AVG(gt.interest_score) as avg_regional_interest,
        MAX(gt.interest_score) as max_regional_interest,
        COUNT(DISTINCT gt.region_id) as regions_with_interest
    FROM {{ source('silver', 'silver_google_trends') }} gt
    GROUP BY gt.keyword_id
),

kvp_with_totals AS (
    SELECT 
        kvp.*
    FROM keyword_video_performance kvp
),

keyword_market_metrics AS (
    SELECT 
        k.keyword_id,
        k.keyword,
        COUNT(DISTINCT k.video_id) as competing_videos,
        COUNT(DISTINCT k.channel_id) as competing_channels,
        AVG(k.view_count) as avg_video_performance,
        MAX(k.view_count) as best_video_performance,
        MIN(k.view_count) as lowest_video_performance,
        AVG(k.engagement_rate) as avg_engagement_rate,
        MAX(k.engagement_rate) as max_engagement_rate
    FROM kvp_with_totals k
    GROUP BY k.keyword_id, k.keyword
)

SELECT 
    kmm.keyword_id,
    kmm.keyword,
    kmm.competing_videos,
    kmm.competing_channels,
    kmm.avg_video_performance,
    kmm.best_video_performance,
    kmm.lowest_video_performance,
    kmm.avg_engagement_rate,
    kmm.max_engagement_rate,
    COALESCE(ktd.avg_regional_interest, 0) as avg_regional_interest,
    COALESCE(ktd.max_regional_interest, 0) as max_regional_interest,
    COALESCE(ktd.regions_with_interest, 0) as regions_with_interest
FROM keyword_market_metrics kmm
LEFT JOIN keyword_trends_data ktd ON kmm.keyword_id = ktd.keyword_id
ORDER BY kmm.avg_video_performance DESC, kmm.avg_engagement_rate DESC