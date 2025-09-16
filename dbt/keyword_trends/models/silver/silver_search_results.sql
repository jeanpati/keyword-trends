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

WITH base AS (
    SELECT 
        s.video_id,
        k.keyword_id, 
        s.title,
        s.description,
        s.channel_id,
        s.channel_title,
        s.publish_time,
        s.searched_at
    FROM {{ source('bronze', 'search_results') }} s
    JOIN {{ ref('silver_keywords') }} k on k.keyword = s.search_keyword
)
SELECT
    ROW_NUMBER() OVER () AS search_result_id,
    *
FROM base