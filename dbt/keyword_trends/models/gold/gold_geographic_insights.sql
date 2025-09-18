
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

regional_interest_data AS (
    SELECT 
        gt.keyword_id,
        k.keyword,
        gt.region_id,
        c.geographic_area_name as region_name,
        gt.interest_score,
        c.total_population,
        c.median_age_years,
        c.total_population_female_percentage,
        c.total_population_male_percentage,
        (c.population_15_to_19_percentage + c.population_20_to_24_percentage) as gen_z_percentage,
        c.population_25_to_34_percentage as millennial_percentage,
        (c.population_35_to_44_percentage + c.population_45_to_54_percentage) as gen_x_percentage,
        (c.population_55_to_59_percentage + c.population_60_to_64_percentage + c.population_65_and_over_percentage) as baby_boomer_plus_percentage,
        ROUND(100 - GREATEST(
            c.white_alone_or_combination_percentage,
            c.black_alone_or_combination_percentage,
            c.asian_alone_or_combination_percentage,
            c.some_other_race_alone_or_combination_percentage
        ), 1) as diversity_index
    FROM top_20_per_keyword sr
    JOIN {{ source('silver', 'silver_google_trends') }} gt ON sr.keyword_id = gt.keyword_id
    JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = gt.keyword_id
    JOIN {{ source('silver', 'silver_census') }} c ON c.region_id = gt.region_id
    WHERE sr.rn <= 20
)

SELECT 
    keyword_id,
    keyword,
    region_id,
    region_name,
    interest_score,
    total_population,
    ROUND(total_population * (interest_score / 100.0), 0) as estimated_interested_population,
    median_age_years,
    total_population_female_percentage,
    total_population_male_percentage,
    gen_z_percentage,
    millennial_percentage,
    gen_x_percentage,
    baby_boomer_plus_percentage,
    diversity_index
FROM regional_interest_data
ORDER BY keyword ASC, interest_score DESC