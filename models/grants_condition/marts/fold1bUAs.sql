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
    -- Unnest data array and match designYear to year_id
    select
        sg."stateId",
        d->>'designYear' as design_year_id,
        round((d->>'currentPrice')::numeric, 2) as "Property tax State GSDP"
    from {{ source('cityfinance_prod','state_gsdp') }} sg,
         lateral jsonb_array_elements(sg.data) as d
),

property_tax_submitted as (
    -- Ensures all ULB-year combinations are covered
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,
        case
            when pt.ulb is null then 'No'                            -- No record at all
            when pt."currentFormStatus" in (4, 6) then 'Yes'         -- Valid form submitted
            else 'No'                                                -- Record exists but not eligible
        end as "Property tax Submitted"
    from ulb_years uy
    left join {{ source('cityfinance_prod','propertytaxops') }} pt
        on uy.ulb_id = pt.ulb and uy.design_year_id = pt.design_year
),

property_tax_mapper as (
    -- Property tax collection value for each ULB and year (displayPriority 1.17)
    select
        ptm.ulb,
        y.year as year_string,
        y._id as year_id,
        case 
            when ptm.value ~ '^(-?(\d+(.\d*)?|.\d+))$' then ptm.value::numeric
            else null
        end as value
    from {{ source('cityfinance_prod','propertytaxopmappers') }} ptm
    left join {{ source('cityfinance_prod','years') }} y
        on ptm.year = y._id
    where ptm."displayPriority" = '1.17'
      --and y.year ~ '^\d{4}-\d{2}$'
),

-- Calculate property tax growth values for T-1 and T-2 years
growth_values as (
    select
        uy.ulb_id,
        uy.design_year,
        g."Property tax State GSDP",
        ptm_A.value as value_A,  -- T-1 value
        ptm_B.value as value_B,   -- T-2 value
        y_A.year as value_A_year,
        y_B.year as value_B_year,
        
        -- Join with property tax submission
        pts."Property tax Submitted",

        -- Central eligibility check
        case
            when pts."Property tax Submitted" = 'Yes'
                 and ptm_B.value is not null and ptm_B.value != 0
                 and ((ptm_A.value - ptm_B.value) / ptm_B.value) * 100 > g."Property tax State GSDP"
            then 'Yes'
            else 'No'
        end as "Eligible for Property Tax Condition"

    from ulb_years uy

    -- Year T-1
    left join years y_A
        on y_A.year = (
            (substring(uy.design_year from 1 for 4)::integer - 1)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 1)::text
        )

    -- Year T-2
    left join years y_B
        on y_B.year = (
            (substring(uy.design_year from 1 for 4)::integer - 2)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 2)::text
        )

    -- Property tax values
    left join property_tax_mapper ptm_A
        on uy.ulb_id = ptm_A.ulb and y_A.year_id = ptm_A.year_id
    left join property_tax_mapper ptm_B
        on uy.ulb_id = ptm_B.ulb and y_B.year_id = ptm_B.year_id

    -- States and GSDP
    left join states s
        on uy.state = s.state_id
    left join state_gsdp g
        on s.state_id = g."stateId"
        and uy.design_year_id = g.design_year_id

    -- Property tax submission status
    left join property_tax_submitted pts
        on uy.ulb_id = pts.ulb and uy.design_year_id = pts.design_year
),

dur_submitted as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year_id,
        case
            when ur.ulb is null then 'No'                              -- No DUR record
            when ur."currentFormStatus" in (4, 6) then 'Yes'           -- Submitted DUR form
            else 'No'                                                  -- Record exists, but not eligible
        end as "DUR Submitted"
    from ulb_years uy
    left join {{ source('cityfinance_prod','utilizationreports') }} ur
        on uy.ulb_id = ur.ulb and uy.design_year_id = ur."designYear"
),

dur_expenditure as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year_id,
        case
            when ds."DUR Submitted" = 'Yes'
                 and ur."grantPosition"->>'expDuringYr' is not null
                 and (ur."grantPosition"->>'expDuringYr')::numeric > 0
            then 'Yes'
            else 'No'
        end as "DUR Greater than 0 Expenditure"
    from ulb_years uy
    left join {{ source('cityfinance_prod','utilizationreports') }} ur
        on uy.ulb_id = ur.ulb and uy.design_year_id = ur."designYear"

    -- Join with DUR submitted status
    left join dur_submitted ds
        on uy.ulb_id = ds.ulb and uy.design_year_id = ds.design_year_id
),

-- SLB submissions
slb_submitted as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,
        case
            when slb.ulb is null then 'No'                            -- No record
            when slb."currentFormStatus" in (4, 6) then 'Yes'         -- Valid form
            else 'No'                                                 -- Record exists but not eligible
        end as "SLB Submission"
    from ulb_years uy
    left join {{ source('cityfinance_prod','twentyeightslbforms') }} slb
        on uy.ulb_id = slb.ulb and uy.design_year_id = slb.design_year
),

-- GFC certifications
gfc_certifications as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,
        case
            when gfc.ulb is null then 'No'                            -- No record
            when gfc."currentFormStatus" in (4, 6) then 'Yes'         -- Valid form
            else 'No'                                                 -- Record exists but not eligible
        end as "GFC Certifications"
    from ulb_years uy
    left join {{ source('cityfinance_prod','gfcformcollections') }} gfc
        on uy.ulb_id = gfc.ulb and uy.design_year_id = gfc.design_year
),

-- ODF certifications
odf_certifications as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,
        case
            when odf.ulb is null then 'No'                            -- No record
            when odf."currentFormStatus" in (4, 6) then 'Yes'         -- Valid form
            else 'No'                                                 -- Record exists but not eligible
        end as "ODF Certifications"
    from ulb_years uy
    left join {{ source('cityfinance_prod','odfformcollections') }} odf
        on uy.ulb_id = odf.ulb and uy.design_year_id = odf.design_year
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
        when a."Annual Accounts Provisional" = 'Yes' 
        and a."Annual Accounts Audited" = 'Yes'
        then 'Yes' else 'No'
    end as "Annual Accounts Both Accounts",

    -- State GSDP value for property tax
    g."Property tax State GSDP",  

    -- Property tax submission status
    p."Property tax Submitted",

    -- Property tax growth rate value A=T-1 & B=T-2 and year
    gv.value_A || ' (' || gv.value_A_year || ')' as "Property Tax T-1",
    gv.value_B || ' (' || gv.value_B_year || ')' as "Property Tax T-2",

    -- Property tax growth rate calculation
    case
        when gv.value_B is not null and gv.value_B != 0 then
            round(((gv.value_A - gv.value_B) / gv.value_B) * 100, 2)
        else null
    end as growth_rate_of_ulb,

    -- ✅ Eligibility precomputed in growth_values
    gv."Eligible for Property Tax Condition" as "Eligible in Property Tax Condition",

    -- DUR and SLB submissions and certifications
    d."DUR Submitted",
    de."DUR Greater than 0 Expenditure",
    s2."SLB Submission",
    gfc."GFC Certifications",
    odf."ODF Certifications",

    -- ✅ UA Grant Eligibility
    case
        when a."Annual Accounts Provisional" = 'Yes'
         and a."Annual Accounts Audited" = 'Yes'
         and gv."Eligible for Property Tax Condition" = 'Yes'
         and d."DUR Submitted" = 'Yes'
         and de."DUR Greater than 0 Expenditure" = 'Yes'
         and s2."SLB Submission" = 'Yes'
         and gfc."GFC Certifications" = 'Yes'
         and odf."ODF Certifications" = 'Yes'
        then 'Yes' else 'No'
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
   and uy.design_year_id = g.design_year_id
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
