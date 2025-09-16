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
WITH year_cte AS (
  SELECT CAST(REGEXP_EXTRACT(column_name, '^[0-9]{4}') AS INT) AS survey_year
  FROM information_schema.columns
  WHERE table_name = 'census'
    AND column_name LIKE '%Estimate%'
  LIMIT 1
),
base AS (
SELECT
    "Geography" as geography_id,
    "Geographic Area Name" as geographic_area_name,
    
    y.survey_year,

    CAST("2024 Estimate!!SEX AND AGE!!Total population" AS BIGINT) as total_population,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Male" AS FLOAT) as total_population_male_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Female" AS FLOAT) as total_population_female_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Sex ratio (males per 100 females)" AS FLOAT) as sex_ratio_males_per_100_females,
    
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Under 5 years" AS FLOAT) as population_under_5_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!5 to 9 years" AS FLOAT) as population_5_to_9_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!10 to 14 years" AS FLOAT) as population_10_to_14_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!15 to 19 years" AS FLOAT) as population_15_to_19_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!20 to 24 years" AS FLOAT) as population_20_to_24_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!25 to 34 years" AS FLOAT) as population_25_to_34_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!35 to 44 years" AS FLOAT) as population_35_to_44_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!45 to 54 years" AS FLOAT) as population_45_to_54_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!55 to 59 years" AS FLOAT) as population_55_to_59_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!60 to 64 years" AS FLOAT) as population_60_to_64_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!65 to 74 years" AS FLOAT) as population_65_to_74_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!75 to 84 years" AS FLOAT) as population_75_to_84_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!85 years and over" AS FLOAT) as population_85_and_ove_percentage,
    
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Median age (years)" AS FLOAT) as median_age_years,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!Under 18 years" AS FLOAT) as population_under_18_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!16 years and over" AS FLOAT) as population_16_and_over_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!18 years and over" AS FLOAT) as population_18_and_over_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!21 years and over" AS FLOAT) as population_21_and_over_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!62 years and over" AS FLOAT) as population_62_and_over_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!65 years and over" AS FLOAT) as population_65_and_over_percentage,
    
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!18 years and over!!Male" AS FLOAT) as population_18_and_over_male_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!18 years and over!!Female" AS FLOAT) as population_18_and_over_female_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!18 years and over!!Sex ratio (males per 100 females)" AS FLOAT) as sex_ratio_18_and_over_males_per_100_females,
    
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!65 years and over!!Male" AS FLOAT) as population_65_and_over_male_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!65 years and over!!Female" AS FLOAT) as population_65_and_over_female_percentage,
    CAST("2024 Estimate!!SEX AND AGE!!Total population!!65 years and over!!Sex ratio (males per 100 females)" AS FLOAT) as sex_ratio_65_and_over_males_per_100_females,
    
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!White" AS FLOAT) as white_alone_or_combination_percentage,
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!Black or African American" AS FLOAT) as black_alone_or_combination_percentage,
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!American Indian and Alaska Native" AS FLOAT) as american_indian_alaska_native_alone_or_combination_percentage,
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!Asian" AS FLOAT) as asian_alone_or_combination_percentage,
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!Native Hawaiian and Other Pacific Islander" AS FLOAT) as native_hawaiian_pacific_islander_alone_or_combination_percentage,
    CAST("2024 Estimate!!Race alone or in combination with one or more other races!!Total population!!Some Other Race" AS FLOAT) as some_other_race_alone_or_combination_percentage,
    
    CAST("2024 Estimate!!Total housing units" AS BIGINT) as total_housing_units,
    
    CAST("2024 Estimate!!CITIZEN, VOTING AGE POPULATION!!Citizen, 18 and over population" AS BIGINT) as citizen_18_and_over_population,
    CAST("2024 Estimate!!CITIZEN, VOTING AGE POPULATION!!Citizen, 18 and over population!!Male" AS FLOAT) as citizen_18_and_over_population_male_percentage,
    CAST("2024 Estimate!!CITIZEN, VOTING AGE POPULATION!!Citizen, 18 and over population!!Female" AS FLOAT) as citizen_18_and_over_population_female_percentage

FROM {{ source('bronze', 'census') }}
CROSS JOIN year_cte y
)
SELECT
    ROW_NUMBER() OVER () AS region_id,
    *
FROM base
