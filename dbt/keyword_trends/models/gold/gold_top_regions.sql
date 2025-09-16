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

WITH keyword_interest_demographics AS (
    SELECT 
        gt.keyword_id,
        k.keyword,
        gt.region_id,
        c.geographic_area_name AS region_name,
        gt.interest_score AS relative_interest_score,
        
        c.total_population,
        c.median_age_years,
        
        c.total_population_female_percentage,
        c.total_population_male_percentage,
        
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
),

top_regions_per_keyword AS (
    SELECT 
        keyword_id,
        keyword,
        region_name,
        MAX(relative_interest_score) AS relative_interest_score,
        MAX(total_population) AS total_population,
        MAX(median_age_years) AS median_age_years,
        
        MAX(total_population_female_percentage) AS total_population_female_percentage,
        MAX(total_population_male_percentage) AS total_population_male_percentage,
        
        MAX(population_15_to_19_percentage) AS population_15_to_19_percentage,
        MAX(population_20_to_24_percentage) AS population_20_to_24_percentage,
        MAX(population_25_to_34_percentage) AS population_25_to_34_percentage,
        MAX(population_35_to_44_percentage) AS population_35_to_44_percentage,
        MAX(population_45_to_54_percentage) AS population_45_to_54_percentage,
        MAX(population_55_to_59_percentage) AS population_55_to_59_percentage,
        MAX(population_60_to_64_percentage) AS population_60_to_64_percentage,
        MAX(population_65_and_over_percentage) AS population_65_and_over_percentage,
        
        MAX(white_alone_or_combination_percentage) AS white_alone_or_combination_percentage,
        MAX(black_alone_or_combination_percentage) AS black_alone_or_combination_percentage,
        MAX(asian_alone_or_combination_percentage) AS asian_alone_or_combination_percentage,
        MAX(american_indian_alaska_native_alone_or_combination_percentage) AS american_indian_alaska_native_alone_or_combination_percentage,
        MAX(native_hawaiian_pacific_islander_alone_or_combination_percentage) AS native_hawaiian_pacific_islander_alone_or_combination_percentage,
        MAX(some_other_race_alone_or_combination_percentage) AS some_other_race_alone_or_combination_percentage
    FROM keyword_interest_demographics
    GROUP BY keyword_id, keyword, region_name
),

ranked_regions AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY keyword_id 
            ORDER BY relative_interest_score DESC, region_name
        ) AS rank
    FROM top_regions_per_keyword
),

keyword_summary AS (
    SELECT 
        keyword_id,
        keyword,
        MAX(CASE WHEN rank = 1 THEN region_name END) AS first_most_interested_region,
        MAX(CASE WHEN rank = 2 THEN region_name END) AS second_most_interested_region,
        MAX(CASE WHEN rank = 3 THEN region_name END) AS third_most_interested_region,
        MAX(relative_interest_score) FILTER (WHERE rank = 1) AS highest_interest_score,
        
        MAX(total_population) FILTER (WHERE rank = 1) AS top_region_total_population,
        MAX(median_age_years) FILTER (WHERE rank = 1) AS top_region_median_age_years,
        MAX(total_population_female_percentage) FILTER (WHERE rank = 1) AS top_region_total_population_female_percentage,
        MAX(total_population_male_percentage) FILTER (WHERE rank = 1) AS top_region_total_population_male_percentage,
        MAX(population_15_to_19_percentage) FILTER (WHERE rank = 1) AS top_region_15_to_19_percentage,
        MAX(population_20_to_24_percentage) FILTER (WHERE rank = 1) AS top_region_20_to_24_percentage,
        MAX(population_25_to_34_percentage) FILTER (WHERE rank = 1) AS top_region_25_to_34_percentage,
        MAX(population_35_to_44_percentage) FILTER (WHERE rank = 1) AS top_region_35_to_44_percentage,
        MAX(population_45_to_54_percentage) FILTER (WHERE rank = 1) AS top_region_45_to_54_percentage,
        MAX(population_55_to_59_percentage) FILTER (WHERE rank = 1) AS top_region_55_to_59_percentage,
        MAX(population_60_to_64_percentage) FILTER (WHERE rank = 1) AS top_region_60_to_64_percentage,
        MAX(population_65_and_over_percentage) FILTER (WHERE rank = 1) AS top_region_65_and_over_percentage,
        MAX(white_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_white_alone_or_combination_percentage,
        MAX(black_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_black_alone_or_combination_percentage,
        MAX(asian_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_asian_alone_or_combination_percentage,
        MAX(american_indian_alaska_native_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_american_indian_alaska_native_alone_or_combination_percentage,
        MAX(native_hawaiian_pacific_islander_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_native_hawaiian_pacific_islander_alone_or_combination_percentage,
        MAX(some_other_race_alone_or_combination_percentage) FILTER (WHERE rank = 1) AS top_region_some_other_race_alone_or_combination_percentage
    FROM ranked_regions
    GROUP BY keyword_id, keyword
)

SELECT 
    keyword,
    first_most_interested_region,
    second_most_interested_region,
    third_most_interested_region,
    
    top_region_total_population,
    top_region_median_age_years,
    top_region_total_population_female_percentage,
    top_region_total_population_male_percentage,
    
    top_region_15_to_19_percentage,
    top_region_20_to_24_percentage,
    top_region_25_to_34_percentage,
    top_region_35_to_44_percentage,
    top_region_45_to_54_percentage,
    top_region_55_to_59_percentage,
    top_region_60_to_64_percentage,
    top_region_65_and_over_percentage,
    
    top_region_white_alone_or_combination_percentage,
    top_region_black_alone_or_combination_percentage,
    top_region_asian_alone_or_combination_percentage,
    top_region_american_indian_alaska_native_alone_or_combination_percentage,
    top_region_native_hawaiian_pacific_islander_alone_or_combination_percentage,
    top_region_some_other_race_alone_or_combination_percentage
FROM keyword_summary
ORDER BY highest_interest_score DESC
