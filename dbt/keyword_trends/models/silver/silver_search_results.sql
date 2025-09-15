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
        video_id,
        search_keyword, 
        title,
        description,
        channel_id,
        channel_title,
        publish_time,
        searched_at
    FROM {{ source('bronze', 'search_results') }}
)
SELECT
    ROW_NUMBER() OVER () AS result_id,
    *
FROM base