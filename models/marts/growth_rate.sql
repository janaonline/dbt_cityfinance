
{{ config(
    materialized='table'
) }}


WITH year_data AS (
    SELECT
        ulb,
        district,
        state,
        year,
        CASE 
            WHEN value IS NULL OR value = '' THEN 0 
            ELSE CAST(value AS FLOAT)
        END as value
    FROM {{ ref('stg_growth_rate') }}
    WHERE 
        type = 'collectIncludingCess'
        AND year IN ('2021-22', '2022-23')
),
pivoted_data AS (
    SELECT 
        ulb,
        district,
        state,
        MAX(CASE WHEN year = '2021-22' THEN value ELSE NULL END) AS value_2021_22,
        MAX(CASE WHEN year = '2022-23' THEN value ELSE NULL END) AS value_2022_23
    FROM year_data
    GROUP BY ulb, district, state
)
SELECT
    ulb,
    district,
    state,
    value_2021_22,
    value_2022_23,
    CASE 
        WHEN value_2021_22 = 0 OR value_2021_22 IS NULL THEN 0
        ELSE ((value_2022_23 - value_2021_22) / value_2021_22) * 100
    END AS growth_rate
FROM pivoted_data

