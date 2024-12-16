{{ config(
    materialized='table'
) }}

with total_cities as (SELECT
        s.name as state,
        u._id as id
    FROM {{ source('cityfinance', 'states') }} s
    Join {{ source('cityfinance','ulbs') }} u
    ON  s._id = u.state 
    where u."isActive" = true
    and s."isUT"= False
)

SELECT
    state,
    COUNT(DISTINCT id) AS total_ulbs
    FROM total_cities
    GROUP BY state