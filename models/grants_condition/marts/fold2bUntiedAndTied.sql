{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}

-- ============================================================================
-- Model: fold2bUntiedAndTied
-- Purpose: 
--   Calculates property tax collection metrics for all active, non-million-plus ULBs,
--   including year-over-year growth and eligibility based on GSDP.
--   Provides both "Total" and "Current" collection metrics for T-2 and T-1 years.
-- ============================================================================

with active_ulbs as (
    -- Select all active ULBs that are not million-plus cities
    select
        _id as ulb_id,
        name,
        state
    from {{ source('cityfinance_prod','ulbs') }} 
    where "isActive" = 'true'
      and "isMillionPlus" = 'No'
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
state_gsdp as (
    -- State GSDP (Gross State Domestic Product) values
    select
        "stateId",
        round((data->0->>'currentPrice')::numeric, 2) as "GSDP"
    from {{ source('cityfinance_prod','state_gsdp') }}
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

total_property_tax_collection as (
    -- Property tax collection for each ULB and year (Total Collection, displayPriority 1.17)
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

current_property_tax_collection as (
    -- Property tax collection for each ULB and year (Current Collection, displayPriority 1.18)
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
    where ptm."displayPriority" = '1.18'
      and y.year ~ '^\d{4}-\d{2}$'
)

select
    -- State and ULB identifiers
    s.state_name as "State Name",
    s.state_id as state_id,
    ic.iso_code as "iso_code",
    uy.ulb_name as "ULB Name",
    uy.design_year as "Year",
    uy.design_year_id as design_year_id,
    g."GSDP" as "GSDP",

    -- Total property tax collection for T-2 and T-1 years
    ttc_B.value as "Total Collection (T-2)",
    ttc_A.value as "Total Collection (T-1)",

    -- Year-over-year growth for Total Collection
    round(
        case 
            when ttc_B.value is not null and ttc_B.value != 0
                then ((ttc_A.value - ttc_B.value) / ttc_B.value) * 100
            else null
        end
    , 2) as "Total Collection State Growth (%)",
    case
        when round(
            case 
                when ttc_B.value is not null and ttc_B.value != 0
                    then ((ttc_A.value - ttc_B.value) / ttc_B.value) * 100
                else null
            end
        , 2) > g."GSDP" then 'Yes'
        else 'No'
    end as "Eligiblity (Total)",

    -- Current property tax collection for T-2 and T-1 years
    ctc_B.value as "Current Collection (T-2)",
    ctc_A.value as "Current Collection (T-1)",

    -- Year-over-year growth for Current Collection
    round(
        case 
            when ctc_B.value is not null and ctc_B.value != 0
                then ((ctc_A.value - ctc_B.value) / ctc_B.value) * 100
            else null
        end
    , 2) as "Current Collection State Growth (%)",

    -- Eligibility based on Current Collection growth vs GSDP
    case
        when round(
            case 
                when ctc_B.value is not null and ctc_B.value != 0
                    then ((ctc_A.value - ctc_B.value) / ctc_B.value) * 100
                else null
            end
        , 2) > g."GSDP" then 'Yes'
        else 'No'
    end as "Eligiblity (Current)"
    
from ulb_years uy
join states s
    on uy.state = s.state_id
left join iso_codes ic
    on s.state_name = ic.state
left join state_gsdp g
    on s.state_id = g."stateId"

-- Join for T-2 year (design_year - 2)
left join years y_B
    on y_B.year = (
        (substring(uy.design_year from 1 for 4)::integer - 2)::text || '-' ||
        lpad((substring(uy.design_year from 6 for 2)::integer - 2)::text, 2, '0')
    )
left join total_property_tax_collection ttc_B
    on uy.ulb_id = ttc_B.ulb and y_B.year_id = ttc_B.year_id    

-- Join for T-1 year (design_year - 1)
left join years y_A
    on y_A.year = (
        (substring(uy.design_year from 1 for 4)::integer - 1)::text || '-' ||
        lpad((substring(uy.design_year from 6 for 2)::integer - 1)::text, 2, '0')
    )
left join total_property_tax_collection ttc_A
    on uy.ulb_id = ttc_A.ulb and y_A.year_id = ttc_A.year_id

-- Join for current collection T-2 and T-1
left join current_property_tax_collection ctc_B
    on uy.ulb_id = ctc_B.ulb and y_B.year_id = ctc_B.year_id
left join current_property_tax_collection ctc_A
    on uy.ulb_id = ctc_A.ulb and y_A.year_id = ctc_A.year_id        

order by s.state_name, uy.ulb_name, uy.design_year



