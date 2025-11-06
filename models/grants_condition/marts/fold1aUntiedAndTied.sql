{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}

-- ============================================================================
-- Model: fold1aUntiedAndTied
-- Purpose:
--   Aggregates compliance and eligibility metrics for all ULBs (excluding UAs)
--   at the state and year level, based on the fold1bUntiedAndTied model.
--   Calculates counts of ULBs meeting different conditions for grants, submissions,
--   and property tax eligibility.
-- ============================================================================

select
    f.state_name,  -- State name for the ULB
    f.iso_code as "iso_code",  -- ISO code for the state
    y.year as "Year",  -- Financial year

    -- Total number of ULBs in the state for the year
    count(f."ULB Name") as "Total ULBs",

    -- Number of ULBs with provisional annual accounts
    count(case when f."Annual Accounts Provisional" = 'Yes' then 1 end) as "Annual Accounts Provisional",

    -- Number of ULBs with audited annual accounts
    count(case when f."Annual Accounts Audited" = 'Yes' then 1 end) as "Annual Accounts Audited",

    -- Number of ULBs with both provisional and audited accounts
    count(case when f."Annual Accounts Both Accounts" = 'Yes' then 1 end) as "Annual Accounts Both Accounts",

    f."Property tax State GSDP",  -- State GSDP value for property tax

    -- Number of ULBs that submitted property tax data
    count(case when f."Property tax Submitted" = 'Yes' then 1 end) as "Property tax Submitted",

    -- Number of ULBs eligible in property tax condition
    count(case when f."Eligible in Property Tax Condition" = 'Yes' then 1 end) as "Eligible in Property Tax Condition",

    -- Number of ULBs that fulfilled all conditions for Un-Tied Grants
    count(case when f."Condition full fill for Un-Tied Grants" = 'Yes' then 1 end) as "Condition full fill for Un-Tied Grants",

    -- Number of ULBs that submitted DUR (Detailed Utilization Report)
    count(case when f."DUR Submitted" = 'Yes' then 1 end) as "DUR Submitted",

    -- Number of ULBs with DUR expenditure greater than 0
    count(case when f."DUR Greater than 0 Expenditure" = 'Yes' then 1 end) as "DUR Greater than 0 Expenditure",

    -- Number of ULBs that submitted Baseline data
    count(case when f."Baseline Submission" = 'Yes' then 1 end) as "Baseline Submission",

    -- Number of ULBs that fulfilled all conditions for Tied Grants
    count(case when f."Condition full fill for Tied Grants" = 'Yes' then 1 end) as "Condition full fill for Tied Grants"

from {{ ref('fold1bUntiedAndTied') }} f
join {{ source('cityfinance_prod','years') }} y
    on f.design_year_id = y._id

group by
    f.state_name,
    f.iso_code,
    y.year,
    f."Property tax State GSDP"

order by
    f.state_name,
    y.year