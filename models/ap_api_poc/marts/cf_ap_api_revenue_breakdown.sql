{{ config(materialized='table', tags=['ap_api_poc']) }}

/*
  MODEL: cf_ap_revenue_breakdown
  
  PURPOSE:
    Classifies and categorizes revenue data from the cf_ap_api_poc source into
    three taxonomies: revenue categories, own source revenue (OSR) sub-categories,
    and property tax classifications.
  
  KEY LOGIC:
    - Each row from cf_ap_api_poc is evaluated against classification rules
    - Only rows matching specific majorcode/subcode combinations are included
    - Three classification dimensions are applied via CASE statements:
      1. revenue: Top-level revenue category (own source, assigned, grants, others)
      2. osr: Own Source Revenue sub-type (tax, fees, sales, rental, others)
      3. property_tax: Property tax line item classification
  
  ASSUMPTIONS:
    - majorcode + subcode combinations uniquely identify line items
    - subcode = 0 indicates main/parent level entries for majorcode 110-180
    - subcodes 1100101-1100104 represent property tax breakdown details
*/

WITH classified_data AS (
    -- Extract base columns and apply classification logic
    SELECT
        majorcode,                              -- Account code (e.g., 110, 120, 130...)
        lineitemname,                           -- Human-readable description
        ulb,                                    -- Urban Local Body identifier
        state,                                  -- State name
        year,                                   -- Fiscal year
        COALESCE(amount, 0) AS amount,         -- Amount (default to 0 if NULL)
        subcode,                                -- Sub-classification code
        
        -- ============================================================================
        -- REVENUE CLASSIFICATION (Top-level categories)
        -- Maps majorcode + subcode combinations to revenue type labels
        -- ============================================================================
        CASE
            -- Own Source Revenue: majorcode 110,130,140,150,180 at main level (subcode=0)
            WHEN majorcode IN (110, 130, 140, 150, 180) AND subcode = 0 
                THEN 'Own Source Revenue'
            
            -- Assigned Revenue: majorcode 120 at main level (subcode=0)
            -- Represents revenue assigned/shared from state to ULB
            WHEN majorcode = 120 AND subcode = 0 
                THEN 'Assigned Revenue'
            
            -- Revenue Grants: majorcode 160 at main level (subcode=0)
            -- Government grants provided for specific purposes
            WHEN majorcode = 160 AND subcode = 0 
                THEN 'Revenue Grants'
            
            -- Other Revenue: majorcode 170,171 at main level (subcode=0)
            -- Miscellaneous revenue sources not in above categories
            WHEN majorcode IN (170, 171) AND subcode = 0 
                THEN 'Other Income'
            
            -- No classification if row doesn't match above patterns
            ELSE NULL
        END AS revenue,
        
        -- ============================================================================
        -- OWN SOURCE REVENUE (OSR) SUB-CLASSIFICATION
        -- Breaks down Own Source Revenue into 5 component types
        -- Only applies to majorcode 110,130,140,150,180 at subcode=0
        -- ============================================================================
        CASE
            -- Tax Revenue: Property tax, business tax, etc.
            WHEN majorcode = 110 AND subcode = 0 
                THEN 'Tax Revenue'
            
            -- Fees and User Charges: License fees, registration, service charges
            WHEN majorcode = 130 AND subcode = 0 
                THEN 'Fees and User Charges'
            
            -- Sales and Hire Charges: Revenue from selling municipal assets/services
            WHEN majorcode = 140 AND subcode = 0 
                THEN 'Sales and Hire Charges'
            
            -- Rental Income: Lease/rent from municipal properties
            WHEN majorcode = 150 AND subcode = 0 
                THEN 'Rental Income'
            
            -- Other OSR: Miscellaneous own source revenue not in above categories
            WHEN majorcode = 180 AND subcode = 0 
                THEN 'Other Income'
            
            -- No classification if outside OSR majorcode range
            ELSE NULL
        END AS osr,
        
        -- ============================================================================
        -- PROPERTY TAX SUB-CLASSIFICATION
        -- Identifies specific property tax line items using subcode values
        -- These subcodes represent detailed property tax components
        -- ============================================================================
        CASE
            -- Property tax detailed breakdown: subcodes 1100101-1100104
            -- These represent individual property tax components/slabs
            WHEN subcode IN (1100101, 1100102, 1100103, 1100104) 
                THEN 'Tax Revenue'
            
            -- No classification if subcode not in property tax range
            ELSE NULL
        END AS property_tax
        
    FROM {{ ref('cf_ap_api_poc') }}
)

-- ============================================================================
-- FINAL SELECTION WITH FILTERING
-- ============================================================================
SELECT
    majorcode,
    lineitemname,
    ulb,
    state,
    year,
    amount,
    subcode,
    revenue,
    osr,
    property_tax,
    to_char(now() AT TIME ZONE 'Asia/Kolkata','FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
FROM classified_data

-- FILTERING LOGIC:
-- Only include rows that match at least one classification criterion
-- This excludes rows with unclassified majorcode/subcode combinations
WHERE (
    -- Revenue category rows: majorcodes 110,130,140,150,180 + 120 + 160 + 170,171 at subcode=0
    (majorcode IN (110, 130, 140, 150, 180) AND subcode = 0)
    OR (majorcode = 120 AND subcode = 0)
    OR (majorcode = 160 AND subcode = 0)
    OR (majorcode IN (170, 171) AND subcode = 0)
    -- Property tax detail rows: specific subcodes regardless of majorcode
    OR subcode IN (1100101, 1100102, 1100103, 1100104)
)

-- Sort by geography, time, and code hierarchy for readability
ORDER BY ulb, state, year, majorcode, subcode