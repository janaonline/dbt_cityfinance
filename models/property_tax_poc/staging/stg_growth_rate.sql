 
{{ config(
    materialized='table'
) }}
 
 with property_tax_mapper as (
    select
        p.ulb,
        p.value,
        p.type,
        p.year
    from {{ source('cityfinance','propertytaxopmappers') }} p
),

ulb_info as (
    select
        u._id as ulb_id,
        u.name,
        u.district,
        u.state
    from {{ source('cityfinance','ulbs') }} u
  
),

year_info as (
    select
        y._id as year_id,
        y.year
    from {{ source('cityfinance','years') }} y
),

state_info as (
    select
        s._id as state_id,
        s.name
    from {{ source('cityfinance','states') }} s
)

select
    u.name as ulb,
    u.district,
    y.year,
    s.name as state,
    p.value,
    p.type
from property_tax_mapper p
join ulb_info u
    on p.ulb = u.ulb_id
join year_info y
    on p.year = y.year_id
join state_info s
    on u.state = s.state_id