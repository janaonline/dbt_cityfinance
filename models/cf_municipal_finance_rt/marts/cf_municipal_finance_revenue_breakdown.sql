{{ config(materialized='table', tags=['cf_municipal_finance_rt']) }}

/*
  MODEL: cf_municipal_finance_revenue_breakdown

  PURPOSE:
    Classifies and categorizes revenue data from the cf_ap_api_poc source into
    three taxonomies: revenue categories, own source revenue (OSR) sub-categories,
    and property tax classifications.

  KEY LOGIC:
    - Each row from cf_ap_api_poc is evaluated against classification rules
    - Only rows matching specific majorcode/nmamcode combinations are included
    - Three classification dimensions are applied via CASE statements:
      1. revenue: Top-level revenue category (own source, assigned, grants, others)
      2. osr: Own Source Revenue sub-type (tax, fees, sales, rental, others)
      3. property_tax: Property tax line item classification

  ASSUMPTIONS:
    - majorcode + nmamcode combinations uniquely identify line items
    - nmamcode = majorcode indicates main/parent level entries for majorcode 110-180
    - nmamcodes 1100101-1100104 represent property tax breakdown details
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
        nmamcode,                               -- Sub-classification code

        -- ============================================================================
        -- REVENUE CLASSIFICATION (Top-level categories)
        -- Maps majorcode + nmamcode combinations to revenue type labels
        -- ============================================================================
        CASE
            -- Own Source Revenue: majorcode 110,130,140,150,180 at main level (nmamcode=majorcode)
            WHEN majorcode IN (110, 130, 140, 150, 180) AND nmamcode = majorcode
                THEN 'Own Source Revenue'

            -- Assigned Revenue: majorcode 120 at main level (nmamcode=majorcode)
            -- Represents revenue assigned/shared from state to ULB
            WHEN majorcode = 120 AND nmamcode = majorcode
                THEN 'Assigned Revenue'

            -- Revenue Grants: majorcode 160 at main level (nmamcode=majorcode)
            -- Government grants provided for specific purposes
            WHEN majorcode = 160 AND nmamcode = majorcode
                THEN 'Revenue Grants'

            -- Other Revenue: majorcode 170,171 at main level (nmamcode=majorcode)
            -- Miscellaneous revenue sources not in above categories
            WHEN majorcode IN (170, 171) AND nmamcode = majorcode
                THEN 'Other Income'

            -- No classification if row doesn't match above patterns
            ELSE NULL
        END AS revenue,

        -- ============================================================================
        -- OWN SOURCE REVENUE (OSR) SUB-CLASSIFICATION
        -- Breaks down Own Source Revenue into 5 component types
        -- Only applies to majorcode 110,130,140,150,180 at nmamcode=majorcode
        -- ============================================================================
        CASE
            -- Tax Revenue: Property tax, business tax, etc.
            WHEN nmamcode IN (1100101, 1100103, 1100105, 1100106, 1100107, 1100201, 1100301, 1100401, 1100501, 1100104)
                THEN 'Property Tax'

            WHEN nmamcode IN (1101101, 1101199, 1101103, 1101104)
                THEN 'Advertisement Tax'

            WHEN nmamcode IN (1105200, 1105201)
                THEN 'Cess'

            WHEN nmamcode IN (1109001, 1109002)
                THEN 'Tax Remission'

            WHEN nmamcode IN (1105101, 1105102, 1105103)
                THEN 'Octroi and toll'

            WHEN nmamcode IN (1109003, 1109004)
                THEN 'Tax refund and early Rebate'

            WHEN nmamcode IN (1100108, 1100701, 1100801, 1101200, 1108001)
                THEN 'Other Tax'

            WHEN nmamcode IN (1100102)
                THEN 'Vacant Land Tax'

            -- Fees and User Charges: License fees, registration, service charges
            WHEN majorcode = 130 AND nmamcode = majorcode
                THEN 'Fees and User Charges'

            -- Sales and Hire Charges: Revenue from selling municipal assets/services
            WHEN majorcode = 140 AND nmamcode = majorcode
                THEN 'Sales and Hire Charges'

            -- Rental Income: Lease/rent from municipal properties
            WHEN majorcode = 150 AND nmamcode = majorcode
                THEN 'Rental Income'

            -- Other OSR: Miscellaneous own source revenue not in above categories
            WHEN majorcode = 180 AND nmamcode = majorcode
                THEN 'Other Income'

            -- No classification if outside OSR majorcode range
            ELSE NULL
        END AS osr,

        -- ============================================================================
        -- PROPERTY TAX SUB-CLASSIFICATION
        -- Identifies specific property tax line items using nmamcode values
        -- These nmamcodes represent detailed property tax components
        -- ============================================================================
        CASE
            -- Property tax detailed breakdown: nmamcodes 1100101-1100104
            -- These represent individual property tax components/slabs
            WHEN nmamcode IN (1100101, 1100103, 1100105, 1100106, 1100107, 1100201, 1100301, 1100401, 1100501, 1100104)
                THEN 'Tax Revenue'

            -- No classification if nmamcode not in property tax range
            ELSE NULL
        END AS property_tax

    FROM {{ ref('cf_municipal_finance_master') }}
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
    nmamcode,
    revenue,
    osr,
    property_tax,
    to_char(now() AT TIME ZONE 'Asia/Kolkata','FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
FROM classified_data

-- FILTERING LOGIC:
-- Only include rows that match at least one classification criterion
-- This excludes rows with unclassified majorcode/nmamcode combinations
WHERE (
    -- Revenue category rows: majorcodes 110,130,140,150,180 + 120 + 160 + 170,171 at nmamcode=majorcode
    (majorcode IN (110, 130, 140, 150, 180) AND nmamcode = majorcode)
    OR (majorcode = 120 AND nmamcode = majorcode)
    OR (majorcode = 160 AND nmamcode = majorcode)
    OR (majorcode IN (170, 171) AND nmamcode = majorcode)
    --OSR sub-category rows: specific nmamcodes for property tax, fees, sales, rental, other tax.
    -- Either of the conditions below can be used to filter rows - using nmamcode directly or checking if osr is not null. Using osr is more robust in case we want to expand the nmamcode list in future without updating the filter.
    --OR nmamcode IN (1100101, 1100102, 1100103, 1100104, 1100105, 1100106, 1100107, 1100108, 1100201, 1100301, 1100401, 1100501, 1100701, 1100801, 1101101, 1101103, 1101104, 1101199, 1101200, 1105101, 1105102, 1105103, 1105200, 1105201, 1108001, 1109001, 1109002, 1109003, 1109004)
    OR revenue IS NOT NULL OR osr IS NOT NULL OR property_tax IS NOT NULL

)

-- Sort by geography, time, and code hierarchy for readability
ORDER BY ulb, state, year, majorcode, nmamcode
