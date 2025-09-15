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
        keyword,
        filename, 
        ingested_at
    FROM {{ source('bronze', 'keywords') }}
)
SELECT
    ROW_NUMBER() OVER () AS keyword_id,
    *
FROM base