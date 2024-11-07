
{{ config(
    materialized='table'
) }}

with state_ulb_counts as (
    select
        state,
        state_code,
        year,
        count(distinct ulb) as total_ulbs
    from {{ ref('stg_tax_sub_rate') }}
    group by state, year, state_code
),

state_approved_ulbs as (
    select
        state,
        year,
        count(distinct ulb) as approved_ulbs
    from {{ ref('stg_tax_sub_rate') }}
    where status = 'APPROVED'
    group by state, year
)

select
    s.state,
    s.year,
    CONCAT('IN-', s.state_code) AS state_code,
    coalesce(a.approved_ulbs, 0) as approved_ulbs,
    s.total_ulbs,
    round(
        coalesce(a.approved_ulbs, 0) * 100.0 / nullif(s.total_ulbs, 0),
        2
    ) as pt_submission_rate
from state_ulb_counts s
left join state_approved_ulbs a
    on s.state = a.state
   and s.year = a.year