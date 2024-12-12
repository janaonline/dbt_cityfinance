{{ config(
    materialized='table'
) }}

with ulb_growth_rate as (
    select
        ulb,
        district,
        state,
        growth_rate
    from {{ ref('growth_rate') }}
),

current_gsdp_rate as (
    select
        s.state,
        s.current_growth_rate
   from {{ source('cityfinance','gsdp_rate') }} s
),
 eligibility as (
    select
        ulb,
        t1.state,
        district,
        growth_rate,
        case
            when t1.growth_rate > t2.current_growth_rate then 1
            else 0
        end as is_eligible
    from ulb_growth_rate t1
    left join current_gsdp_rate t2
        on t1.state = t2.state
),

state_summary as (
    select
        state,
        sum(is_eligible) as eligible_cities,
        count(ulb) as total_cities
    from eligibility
    group by state
),

total_ulbs as (SELECT
        s.name as state,
        u._id as id
    FROM {{ source('cityfinance', 'states') }} s
    Join {{ source('cityfinance','ulbs') }} u
    ON  s._id = u.state 
    where u."isActive" = true
    and s."isUT"= False
)
,

eligible_cities as(
    SELECT 
    ulb_counts.state,
    COALESCE(s.eligible_cities, 0) AS eligible_cities,
    COALESCE(ulb_counts.total_cities, 0) AS total_cities,
    round(COALESCE(s.eligible_cities, 0) * 100.0 / nullif(ulb_counts.total_cities, 0), 2) as eligibility_percentage
from state_summary s
right Join (SELECT
        state,
        COUNT(DISTINCT id) AS total_cities
    FROM total_ulbs
    GROUP BY state
) ulb_counts ON s.state = ulb_counts.state
)

SELECT e.state, eligible_cities, total_cities, eligibility_percentage, iso_code as state_code
 from eligible_cities e join {{ source('cityfinance','iso_codes') }} i
 on e.state = i.state
    