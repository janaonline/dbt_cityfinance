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
)

select
    t1.ulb,
    t1.district,
    t1.state,
    case
        when t1.growth_rate > t2.current_growth_rate then 'Yes'
        else 'No'
    end as is_eligible
from ulb_growth_rate t1
left join current_gsdp_rate t2
    on t1.state = t2.state