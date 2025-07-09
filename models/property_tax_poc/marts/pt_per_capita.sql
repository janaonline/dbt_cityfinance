
{{ config(
    materialized='table'
) }}


WITH tax_collected AS (
    SELECT
        ulb,
        district,
        state,
        year,
        CASE 
            WHEN value IS NULL OR value = '' THEN 0 
            ELSE CAST(value AS FLOAT)
        END as total_pt_collected
    FROM {{ ref('stg_growth_rate') }}
    WHERE 
        type = 'collectIncludingCess'
),



pt_per_capita AS (
    SELECT
        tc.ulb,
        tc.district,
        tc.state,
        tc.year,
        tc.total_pt_collected,
        pd.population,
        CASE 
            WHEN pd.population > 0 THEN tc.total_pt_collected / pd.population
            ELSE NULL -- Handle division by zero if population is zero or missing
        END AS pt_per_capita
    FROM tax_collected AS tc
    LEFT JOIN  {{ ref('stg_population_data') }} AS pd
    ON tc.ulb = pd.ulb
)

SELECT * 
FROM pt_per_capita