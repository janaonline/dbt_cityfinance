{{ config(
    materialized='table'
) }}
 
with population_data AS (
    SELECT
        p."D9191_2" as ulb,
        p."StateName" as state,
        COALESCE(CAST("I9191_4"->>'TotalPopulationWeight' AS FLOAT), 0) AS population -- Default to 0 if population is NULL
    FROM {{ source('cityfinance','population') }} p 
)

select * from population_data