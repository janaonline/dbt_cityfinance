{{ config(
    materialized='table'
) }}

WITH base_data AS (
    SELECT
        ulb,
        district,
        state,
        year,
        type,
        CASE 
            WHEN value IS NULL OR value = '' THEN 0 
            ELSE CAST(value AS FLOAT)
        END as value
    FROM {{ ref('stg_growth_rate') }}
),

collection_and_demand AS (
    SELECT
        ulb,
        district,
        state,
        year,
        SUM(CASE WHEN type = 'collectIncludingCess' THEN value ELSE 0 END) AS total_pt_collected,
        SUM(CASE WHEN type = 'arCollectIncludingCess' THEN value ELSE 0 END) AS total_pt_demand
    FROM base_data
    GROUP BY ulb, district, state, year
),

pt_collection_percentage AS (
    SELECT
        ulb,
        district,
        state,
        year,
        total_pt_collected,
        total_pt_demand,
        CASE 
            WHEN total_pt_demand > 0 THEN (total_pt_collected / total_pt_demand) * 100
            ELSE NULL 
        END AS pt_collection_percentage
    FROM collection_and_demand
)

SELECT * 
FROM pt_collection_percentage