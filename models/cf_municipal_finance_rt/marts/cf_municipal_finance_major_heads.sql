-- Configuration: Materialize as a table for improved query performance
-- Tagged with 'cf_municipal_finance_rt' for easy identification and selective execution
{{ config(materialized='table', tags=['cf_municipal_finance_rt']) }}

-- Purpose: Aggregate financial data at the major code level
-- Description: This mart table rolls up detailed AP API data to major classification level,
--              filtering out sub-items (nmamCode = majorCode) to show only top-level categories
--
-- Data includes: Major code groupings, line item names, amounts, and geographic/temporal dimensions
-- Updated timestamp in IST (Indian Standard Time) for consistency across deployments

SELECT
    majorCode,      -- Major classification code for expense categorization
    lineItemName,   -- Human-readable name of the line item
    Amount,         -- Financial amount in the transaction
    ulb,            -- Urban Local Body identifier
    state,          -- State code or name
    year,           -- Fiscal year for the transaction

    -- Derive head of account based on major code patterns
    CASE
    WHEN majorCode::text LIKE '1%' THEN 'Revenue'
    WHEN majorCode::text LIKE '2%' THEN 'Expenditure'
    WHEN majorCode::text LIKE '3%' THEN 'Liability'
    WHEN majorCode::text LIKE '4%' THEN 'Asset'
    ELSE 'Other'
    END AS "headOfAccount",
    
    -- Generate current server timestamp in IST with readable format
    -- Format: "Month DD YYYY at HH12:MI am" (e.g., "January 15 2026 at 03:45 pm")
    to_char(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
    
FROM {{ ref('cf_municipal_finance_master') }}

-- Filter condition: Include only major-level records (nmamCode = majorCode)
-- This excludes detail rows and prevents double-counting in aggregations
WHERE nmamCode = majorCode