{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}

-- ============================================================================
-- Model: fold1aUAs
-- Purpose:
--   Aggregates various compliance and eligibility metrics for Urban Agglomerations (UAs)
--   at the state and year level, based on the fold1bUAs model.
--   Calculates counts of ULBs meeting different conditions for grants and submissions.
-- ============================================================================

select
    f."State Name",  -- State name for the UA
    f.iso_code as "iso_code",  -- ISO code for the state
    y.year as "Year",  -- Financial year

    -- Total number of ULBs in the UA for the year
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

    -- Number of ULBs that submitted DUR (Detailed Utilization Report)
    count(case when f."DUR Submitted" = 'Yes' then 1 end) as "DUR Submitted",

    -- Number of ULBs with DUR expenditure greater than 0
    count(case when f."DUR Greater than 0 Expenditure" = 'Yes' then 1 end) as "DUR Greater than 0 Expenditure",

    -- Number of ULBs that submitted SLB (Service Level Benchmark) data
    count(case when f."SLB Submission" = 'Yes' then 1 end) as "SLB Submission",

    -- Number of ULBs with GFC (General Financial Compliance) certifications
    count(case when f."GFC Certifications" = 'Yes' then 1 end) as "GFC Certifications",

    -- Number of ULBs with ODF (Open Defecation Free) certifications
    count(case when f."ODF Certifications" = 'Yes' then 1 end) as "ODF Certifications",

    -- Number of ULBs that fulfilled all conditions for UA grants
    count(case when f."Condition full fill for UA Grants" = 'Yes' then 1 end) as "Condition full fill for UA Grants"

from {{ ref('fold1bUAs') }} f
join {{ source('cityfinance_prod','years') }} y
    on f.design_year_id = y._id

group by
    f."State Name",
    f.iso_code,
    y.year,
    f."Property tax State GSDP"

order by
    f."State Name",
    y.year