{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH base_data AS (
    SELECT 
        ulb_name, 
        ulb_code, 
        state_name, 
        iso_code,
        population_category,
        population_category_sort_order,
        population_category_ordered,
        financial_year,
        is_standadized_by_magc
    FROM {{ ref('afs_annual_account_eligibility') }}
)

SELECT
    ulb_name,
    ulb_code,
    state_name,
    iso_code,
    population_category,
    population_category_sort_order,
    population_category_ordered,
    1 AS total_count,
    -- 1. 3 year contiguous data (2019-20 to 2021-22)
    CASE 
        WHEN COUNT(CASE WHEN financial_year IN ('2019-20', '2020-21', '2021-22') AND is_standadized_by_magc = 'standardized' THEN 1 END) = 3 
        THEN 1 
        ELSE 0 
    END AS is_3_year_contiguous,
    
    -- 2. 4 year contiguous data (2019-20 to 2022-23)
    CASE 
        WHEN COUNT(CASE WHEN financial_year IN ('2019-20', '2020-21', '2021-22', '2022-23') AND is_standadized_by_magc = 'standardized' THEN 1 END) = 4 
        THEN 1 
        ELSE 0 
    END AS is_4_year_contiguous,
    
    -- 3. 5 year contiguous data (2019-20 to 2023-24)
    CASE 
        WHEN COUNT(CASE WHEN financial_year IN ('2019-20', '2020-21', '2021-22', '2022-23', '2023-24') AND is_standadized_by_magc = 'standardized' THEN 1 END) = 5 
        THEN 1 
        ELSE 0 
    END AS is_5_year_contiguous,

    -- Runtime timestamp for dashboard freshness
    TO_CHAR(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
FROM base_data
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8