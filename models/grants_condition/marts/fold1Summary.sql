{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}


with active_ulbs as (
    -- Select all active ULBs (no million-plus filter)
    select
        _id as ulb_id,
        name,
        state
    from {{ source('cityfinance_prod','ulbs') }}
    where "isActive" = 'true'
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
ulb_count as (
    -- Count of active ULBs per state (across all years)
    select
        s.state_name as "State Name",
        count(a.ulb_id) as "No. of ULBs"
    from active_ulbs a
    join states s
        on a.state = s.state_id
    group by s.state_name
)

select
    -- Name of the state (e.g., Karnataka, Maharashtra)
    n."State Name",

    -- ISO code: A unique identifier for each state (e.g., KA for Karnataka)
    n.iso_code,

    -- Financial year (e.g., 2022, 2023)
    n."Year",

    -- Total number of active ULBs in the state (from ulbs collection, all years)
    uc."No. of ULBs",

    -- Total number of NMPC ULBs (Non-Municipal Population Centres) in the state and year.
    -- ULB: Urban Local Body, a local government entity in urban areas.
    -- NMPC: Urban areas not governed by a municipal corporation but recognized for grants.
    coalesce(n."Total ULBs", 0) as "No. of NMPCs",

    -- Total number of UA ULBs (Urban Agglomerations) in the state and year.
    -- UA: Urban Agglomeration, a group of physically contiguous towns/ULBs.
    coalesce(u."Total ULBs", 0) as "No. of UAs",

    -- Percentage of NMPC ULBs eligible for Un-Tied Grants.
    -- Un-Tied Grants: Funds with flexible usage, given if certain conditions are met.
    -- "Condition full fill for Un-Tied Grants": Number of NMPC ULBs that met eligibility.
    -- Calculation: (Eligible NMPCs / Total NMPCs) * 100, rounded to 2 decimals.
    -- Handles division by zero by returning 0 if there are no NMPCs.
    case 
        when coalesce(n."Total ULBs", 0) = 0 then 0
        else round(coalesce(n."Condition full fill for Un-Tied Grants", 0)::numeric / coalesce(n."Total ULBs", 0) * 100, 2)
    end as "Un-Tied - % of eligible ULBs",

    -- Percentage of NMPC ULBs eligible for Tied Grants.
    -- Tied Grants: Funds for specific purposes (e.g., water, sanitation), stricter conditions.
    -- "Condition full fill for Tied Grants": Number of NMPC ULBs that met tied grant eligibility.
    case 
        when coalesce(n."Total ULBs", 0) = 0 then 0
        else round(coalesce(n."Condition full fill for Tied Grants", 0)::numeric / coalesce(n."Total ULBs", 0) * 100, 2)
    end as "Tied - % of eligible ULBs",

    -- Percentage of UA ULBs eligible for UA Grants.
    -- "Condition full fill for UA Grants": Number of UA ULBs that met UA grant eligibility.
    -- Calculation: (Eligible UAs / Total UAs) * 100, rounded to 2 decimals.
    case 
        when coalesce(u."Total ULBs", 0) = 0 then 0
        else round(coalesce(u."Condition full fill for UA Grants", 0)::numeric / coalesce(u."Total ULBs", 0) * 100, 2)
    end as "UAs - % of eligible ULBs",

    -- Add current server date and time in the requested format
    to_char(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
    
from 
    -- Main NMPC summary table (from fold1aUntiedAndTied model)
    {{ ref('fold1aUntiedAndTied') }} n

    -- Join with UA summary table (from fold1aUAs model) on state, iso_code, and year
    left join {{ ref('fold1aUAs') }} u
        on n."State Name" = u."State Name"
        and n.iso_code = u.iso_code
        and n."Year" = u."Year"
    -- Join with ulb_count to get total active ULBs per state
    left join ulb_count uc
        on n."State Name" = uc."State Name"

-- Order results by state and year for easier analysis
order by n."State Name", n."Year"

/*
    Model: fold1Summary.sql

    Purpose:
    -----------
    This model generates a summary table showing, for each state and year:
      - The total number of active ULBs in the state (all years)
      - The total number of NMPC ULBs and UA ULBs
      - The percentage of ULBs eligible for Un-Tied, Tied, and UA grants

    Technical Terms:
    ---------------
    - ULB (Urban Local Body): Local government body for urban areas (municipalities, corporations, etc.)
    - NMPC (Non-Municipal Population Centre): Urban area not governed by a municipal corporation.
    - UA (Urban Agglomeration): Group of physically connected towns/ULBs.
    - ISO code: Standardized code to uniquely identify each state.
    - Un-Tied Grants: Flexible grants for ULBs, subject to eligibility.
    - Tied Grants: Grants for specific purposes, stricter eligibility.
    - Condition full fill for ... Grants: Number of ULBs that met all requirements for that grant.
    - COALESCE: SQL function to replace NULLs with a default value (here, 0).

    Usage:
    ------
    Use this summary for reporting and dashboarding to monitor grant eligibility coverage
    across states and years.
*/