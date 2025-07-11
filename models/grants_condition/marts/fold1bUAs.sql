{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}

-- ============================================================================
-- Model: fold1bUAs
-- Purpose:
--   Calculates compliance and eligibility metrics for Urban Agglomerations (UAs)
--   at the ULB and year level, including property tax growth, account status,
--   DUR/SLB/ODF/GFC submissions, and overall grant eligibility.
-- ============================================================================

with active_ulbs as (
    -- Select all active ULBs that are Urban Agglomerations (UAs)
    select
        _id as ulb_id,
        name,
        state
    from {{ source('cityfinance_prod','ulbs') }} 
    where "isActive" = 'true'
      and "isUA" = 'Yes'
),

states as (
    -- Select all states except UTs and test states
    select
        _id as state_id,
        name as state_name
    from {{ source('cityfinance_prod','states') }} 
    where "isUT" = 'false'
      and name != 'TEST STATE'
),

iso_codes as (
    -- ISO codes for states
    select
        state,
        iso_code
    from {{ source('cityfinance_prod','iso_codes') }}
),

years as (
    -- All years from the years table
    select
        _id as year_id,
        year
    from {{ source('cityfinance_prod','years') }}
),

ulb_years as (
    -- Create a matrix of all active ULBs and all years
    select
        u.ulb_id,
        u.name as ulb_name,
        u.state,
        y.year_id as design_year_id,
        y.year as design_year
    from active_ulbs u
    cross join years y
),

annual_accounts as (
    -- Annual accounts provisional and audited status for each ULB and year
    select
        ulb,
        design_year,
        case 
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
              and ("unAudited"->>'submit_annual_accounts')::boolean = true
            then 'Yes'
            else 'No'
        end as "Annual Accounts Provisional",
        case 
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
              and ("audited"->>'submit_annual_accounts')::boolean = true
            then 'Yes'
            else 'No'
        end as "Annual Accounts Audited"
    from {{ source('cityfinance_prod','annualaccountdatas') }}
),

state_gsdp as (
    -- State GSDP (Gross State Domestic Product) for property tax
    select
        "stateId",
        round((data->0->>'currentPrice')::numeric, 2) as "Property tax State GSDP"
    from {{ source('cityfinance_prod','state_gsdp') }}
),

property_tax_submitted as (
    -- Property tax submission status for each ULB and year
    select
        ulb,
        design_year,
        case
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
            then 'Yes'
            else 'No'
        end as "Property tax Submitted"
    from {{ source('cityfinance_prod','propertytaxops') }}
),

property_tax_mapper as (
    -- Property tax collection value for each ULB and year (displayPriority 1.17)
    select
        ptm.ulb,
        y.year as year_string,
        y._id as year_id,
        case 
            when ptm.value ~ '^\d+(\.\d+)?$' then ptm.value::numeric
            else null
        end as value
    from {{ source('cityfinance_prod','propertytaxopmappers') }} ptm
    left join {{ source('cityfinance_prod','years') }} y
        on ptm.year = y._id
    where ptm."displayPriority" = '1.17'
      and y.year ~ '^\d{4}-\d{2}$'
),

-- Calculate property tax growth values for T-1 and T-2 years
growth_values as (
    select
        uy.ulb_id,
        uy.design_year,
        g."Property tax State GSDP",
        ptm_A.value as value_A,  -- T-1 value
        ptm_B.value as value_B   -- T-2 value
    from ulb_years uy
    -- Find year_id for A (design_year - 1)
    left join years y_A
        on y_A.year = (
            (substring(uy.design_year from 1 for 4)::integer - 1)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 1)::text
        )
    -- Find year_id for B (design_year - 2)
    left join years y_B
        on y_B.year = (
            (substring(uy.design_year from 1 for 4)::integer - 2)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 2)::text
        )
    -- Get property tax values for A and B
    left join property_tax_mapper ptm_A
        on uy.ulb_id = ptm_A.ulb and y_A.year_id = ptm_A.year_id
    left join property_tax_mapper ptm_B
        on uy.ulb_id = ptm_B.ulb and y_B.year_id = ptm_B.year_id
    left join states s
        on uy.state = s.state_id
    left join state_gsdp g
        on s.state_id = g."stateId"
),

dur_submitted as (
    -- DUR (Detailed Utilization Report) submission status
    select
        ulb,
        "designYear" as design_year_id,
        case
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
            then 'Yes'
            else 'No'
        end as "DUR Submitted"
    from {{ source('cityfinance_prod','utilizationreports') }}
),

dur_expenditure as (
    -- DUR expenditure greater than 0 status
    select
        ulb,
        "designYear" as design_year_id,
        case
            when ("grantPosition"->>'expDuringYr')::numeric > 0
            then 'Yes'
            else 'No'
        end as "DUR Greater than 0 Expenditure"
    from {{ source('cityfinance_prod','utilizationreports') }}
),

slb_submitted as (
    -- SLB (Service Level Benchmark) submission status
    select
        ulb,
        design_year,
        case
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
            then 'Yes'
            else 'No'
        end as "SLB Submission"
    from {{ source('cityfinance_prod','twentyeightslbforms') }}
),

gfc_certifications as (
    -- GFC (General Financial Compliance) certifications status
    select
        ulb,
        design_year,
        case
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
            then 'Yes'
            else 'No'
        end as "GFC Certifications"
    from {{ source('cityfinance_prod','gfcformcollections') }}
),

odf_certifications as (
    -- ODF (Open Defecation Free) certifications status
    select
        ulb,
        design_year,
        case
            when ("currentFormStatus" = 4 or "currentFormStatus" = 6)
            then 'Yes'
            else 'No'
        end as "ODF Certifications"
    from {{ source('cityfinance_prod','odfformcollections') }}
)

select
    s.state_name as "State Name",  -- State name for the ULB
    s.state_id as state_id,        -- State ID
    ic.iso_code as "iso_code",     -- ISO code for the state
    uy.ulb_name as "ULB Name",     -- ULB name
    uy.design_year as "Year",      -- Financial year
    uy.design_year_id as design_year_id,  -- Year ID

    -- Annual accounts provisional and audited status
    a."Annual Accounts Provisional",
    a."Annual Accounts Audited",

    -- Both provisional and audited accounts present
    case
        when a."Annual Accounts Provisional" = 'Yes' and a."Annual Accounts Audited" = 'Yes'
        then 'Yes'
        else 'No'
    end as "Annual Accounts Both Accounts",

    -- State GSDP value for property tax
    g."Property tax State GSDP",  

    -- Property tax submission status
    p."Property tax Submitted",

    -- Property tax growth rate calculation
    case
        when gv.value_B is not null and gv.value_B != 0 then
            ((gv.value_A - gv.value_B) / gv.value_B) * 100
        else null
    end as growth_rate_of_ulb,

    -- Eligibility in property tax condition (growth > GSDP)
    case
        when gv.value_B is not null and gv.value_B != 0 and ((gv.value_A - gv.value_B) / gv.value_B) * 100 > g."Property tax State GSDP"
        then 'Yes'
        else 'No'
    end as "Eligible in Property Tax Condition",

    -- DUR and SLB submissions and certifications
    d."DUR Submitted",
    de."DUR Greater than 0 Expenditure",
    s2."SLB Submission",
    gfc."GFC Certifications",
    odf."ODF Certifications",

    -- Fulfillment of all conditions for UA grants
    case
        when 
            (a."Annual Accounts Provisional" = 'Yes' and a."Annual Accounts Audited" = 'Yes')
            and (gv.value_B is not null and gv.value_B != 0 and ((gv.value_A - gv.value_B) / gv.value_B) * 100 > g."Property tax State GSDP")
            and d."DUR Submitted" = 'Yes'
            and de."DUR Greater than 0 Expenditure" = 'Yes'
            and s2."SLB Submission" = 'Yes'
            and gfc."GFC Certifications" = 'Yes'
            and odf."ODF Certifications" = 'Yes'
        then 'Yes'
        else 'No'
    end as "Condition full fill for UA Grants"  

from ulb_years uy
join states s
    on uy.state = s.state_id
left join iso_codes ic
    on s.state_name = ic.state      
left join annual_accounts a
    on uy.ulb_id = a.ulb
    and uy.design_year_id = a.design_year
left join state_gsdp g
    on s.state_id = g."stateId"
left join property_tax_submitted p
    on uy.ulb_id = p.ulb
    and uy.design_year_id = p.design_year
left join growth_values gv
    on uy.ulb_id = gv.ulb_id
    and uy.design_year = gv.design_year
left join dur_submitted d
    on uy.ulb_id = d.ulb
    and uy.design_year_id = d.design_year_id
left join dur_expenditure de
    on uy.ulb_id = de.ulb
    and uy.design_year_id = de.design_year_id   
left join slb_submitted s2
    on uy.ulb_id = s2.ulb
    and uy.design_year_id = s2.design_year  
left join gfc_certifications gfc
    on uy.ulb_id = gfc.ulb
    and uy.design_year_id = gfc.design_year   
left join odf_certifications odf
    on uy.ulb_id = odf.ulb
    and uy.design_year_id = odf.design_year
