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


SELECT 
    gt.keyword_id,
    k.keyword,
    gt.region_id,
    c.geographic_area_name as region_name,
    gt.interest_score as relative_interest_score,
    gt.begin_date,
    gt.end_date,
    
    c.total_population,
    c.median_age_years,
    
    c.total_population_female_percentage,
    c.total_population_male_percentage,
    c.population_18_and_over_female_percentage,
    c.population_18_and_over_male_percentage,
    
    c.population_15_to_19_percentage,
    c.population_20_to_24_percentage,
    c.population_25_to_34_percentage,
    c.population_35_to_44_percentage, 
    c.population_45_to_54_percentage,
    c.population_55_to_59_percentage,
    c.population_60_to_64_percentage,
    c.population_65_and_over_percentage,
    
    c.white_alone_or_combination_percentage,
    c.black_alone_or_combination_percentage,
    c.asian_alone_or_combination_percentage,
    c.american_indian_alaska_native_alone_or_combination_percentage,
    c.native_hawaiian_pacific_islander_alone_or_combination_percentage,
    c.some_other_race_alone_or_combination_percentage        
FROM {{ source('silver', 'silver_google_trends') }} gt
JOIN {{ source('silver', 'silver_keywords') }} k ON k.keyword_id = gt.keyword_id
JOIN {{ source('silver', 'silver_census') }} c ON c.region_id = gt.region_id
