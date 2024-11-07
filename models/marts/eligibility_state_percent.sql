{{ config(
    materialized='table'
) }}

with ulb_growth_rate as (
    select
        ulb,
        district,
        state_code,
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
        t1.state_code,
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
        state_code,
        sum(is_eligible) as eligible_cities,
        count(ulb) as total_cities
    from eligibility
    group by state, state_code
)

select
    state,
    state_code,
    eligible_cities,
    total_cities,
    round(eligible_cities * 100.0 / nullif(total_cities, 0), 2) as eligibility_percentage
from state_summary