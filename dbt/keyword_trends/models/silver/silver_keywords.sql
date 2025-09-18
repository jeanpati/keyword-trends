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

WITH base AS (
    SELECT 
        keyword,
        filename, 
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM {{ source('bronze', 'keywords') }}
)
SELECT
    ROW_NUMBER() OVER () AS keyword_id,
    *
FROM base