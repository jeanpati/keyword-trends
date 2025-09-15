{{ config(
    materialized='table',
    pre_hook=[
        "SET s3_endpoint='localhost:9000'",
        "SET s3_use_ssl=false", 
        "SET s3_access_key_id='admin'",
        "SET s3_secret_access_key='password'",
        "SET s3_url_style='path'",
        "ATTACH 'ducklake:/Users/jean/workspace/data-engineering/capstone-project-keyword-trends/data/ducklake-db/catalog.duckdb' AS trends_lake (DATA_PATH 's3://bronze/parquet/')",
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
    ROW_NUMBER() OVER () AS keyword_id,   -- surrogate key
    *
FROM base