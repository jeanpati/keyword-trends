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
        k.keyword_id,
        c.region_id,
        t.interest_score,
        t.begin_date,
        t.end_date,
        t.filename,
        CAST(t.retrieved_at AS TIMESTAMP) AS retrieved_at
    FROM {{ source('bronze', 'google_trends') }} t
    JOIN {{ ref('silver_census') }} c on c.geographic_area_name = t.region
    JOIN {{ ref('silver_keywords') }} k on k.keyword = t.keyword
)
SELECT
    ROW_NUMBER() OVER () AS trend_id,
    *
FROM base