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


SELECT 
    video_id,
    view_count,
    like_count,
    comment_count,
    duration,
    (
        COALESCE(CAST(NULLIF(REGEXP_EXTRACT(duration, 'PT(\d+)H', 1), '') AS INT), 0) * 3600 +
        COALESCE(CAST(NULLIF(REGEXP_EXTRACT(duration, '(\d+)M', 1), '') AS INT), 0) * 60 +
        COALESCE(CAST(NULLIF(REGEXP_EXTRACT(duration, '(\d+)S', 1), '') AS INT), 0)
    ) AS duration_seconds,
    CAST(retrieved_at AS TIMESTAMP) AS retrieved_at
FROM {{ source('bronze', 'video_statistics') }}
