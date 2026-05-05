{{ config(materialized = 'table', tags = ['correlation_readiness']) }}

{%- set columns = adapter.get_columns_in_relation(source('correlation_readiness', 'ledgerlogs')) -%}
{%- set column_names = columns | map(attribute='name') | list -%}


WITH states AS (
    SELECT _id, name 
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

ulbs AS (
    SELECT 
        u._id AS ulb_id, 
        u.name AS ulb_name, 
        u.state AS state_id, 
        u.code AS ulb_code,

        CASE
            WHEN u.population < 100000 THEN '<100K'
            WHEN u.population < 500000 THEN '100K-500K'
            WHEN u.population < 1000000 THEN '500K-1M'
            WHEN u.population < 4000000 THEN '1M-4M'
            WHEN u.population >= 4000000 THEN '4M+'
            ELSE 'NA'
        END AS population_category,

        CASE
            WHEN u.population >= 4000000 THEN 1
            WHEN u.population >= 1000000 THEN 2
            WHEN u.population >= 500000 THEN 3
            WHEN u.population >= 100000 THEN 4
            WHEN u.population < 100000 THEN 5
            ELSE 99
        END AS population_category_sort_order,

        CASE
            WHEN u.population >= 4000000 THEN '01 - 4M+'
            WHEN u.population >= 1000000 THEN '02 - 1M-4M'
            WHEN u.population >= 500000 THEN '03 - 500K-1M'
            WHEN u.population >= 100000 THEN '04 - 100K-500K'
            WHEN u.population < 100000 THEN '05 - <100K'
            ELSE '99 - NA'
        END AS population_category_ordered
        
    FROM {{ source('cityfinance_prod', 'ulbs') }} u
    INNER JOIN states s ON u.state = s._id
    WHERE u."isActive" = 'true' 
      AND u."isPublish" = 'true'
),

iso_codes AS (
    SELECT state, iso_code
    FROM {{ source('cityfinance_prod','iso_codes') }}
),

years AS (
    SELECT 
        _id AS year_id,
        year AS financial_year,
        CAST(SPLIT_PART(year, '-', 1) AS INTEGER) AS financial_year_start
    FROM {{ source('cityfinance_prod', 'years') }}
),

ulb_year_base AS (
    SELECT
        u.ulb_id,
        u.ulb_name,
        u.ulb_code,
        s.name AS state_name,
        i.iso_code,
        u.population_category,
        u.population_category_sort_order,
        u.population_category_ordered,
        y.year_id,
        y.financial_year,
        y.financial_year_start
    FROM ulbs u
    CROSS JOIN years y
    LEFT JOIN states s ON u.state_id = s._id
    LEFT JOIN iso_codes i ON s.name = i.state
),

standardization_logs AS (
    SELECT 
        ulb_id::TEXT AS ulb_id,
        year::TEXT AS year_key,

        COUNT(*) AS standardization_record_count,

        BOOL_OR(
            COALESCE(LOWER(BTRIM("isStandardizable"::TEXT)), '') <> 'no'
        ) AS has_standardized_value,

        BOOL_OR(
            LOWER(BTRIM("isStandardizable"::TEXT)) = 'no'
        ) AS has_error_value,

     -- ADDED: market_ready_status from indicators JSON -> marketReadinessBand   
    MAX(
            NULLIF(
                ("marketReadinessScore"::jsonb ->> 'marketReadinessBand'),
                ''
            )
        ) AS market_readiness_band,

      -- ADDED: Total Revenue from indicators JSON -> totRevenue     
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("indicators"::TEXT, '')::jsonb ->> 'totRevenue'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("indicators"::TEXT, '')::jsonb 
                    ->> 'totRevenue'
                )::NUMERIC
                ELSE NULL
            END
        ) AS total_revenue,

        -- ADDED: Revenue Expenditure from indicators JSON -> totRevenueExpenditure
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("indicators"::TEXT, '')::jsonb ->> 'totRevenueExpenditure'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("indicators"::TEXT, '')::jsonb 
                    ->> 'totRevenueExpenditure'
                )::NUMERIC
                ELSE NULL
            END
        ) AS revenue_expenditure,

        -- ADDED: CapEx from indicators JSON -> capex
        -- If capex is 'N/A', it will return NULL instead of causing a casting error
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("indicators"::TEXT, '')::jsonb ->> 'capex'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("indicators"::TEXT, '')::jsonb 
                    ->> 'capex'
                )::NUMERIC
                ELSE NULL
            END
        ) AS capex,

        -- ADDED: line item 240 from lineItems JSON
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("lineItems"::TEXT, '')::jsonb ->> '240'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("lineItems"::TEXT, '')::jsonb 
                    ->> '240'
                )::NUMERIC
                ELSE NULL
            END
        ) AS  Interest_Finance_Charges,

        -- ADDED: line item 272 from lineItems JSON
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("lineItems"::TEXT, '')::jsonb ->> '272'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("lineItems"::TEXT, '')::jsonb 
                    ->> '272'
                )::NUMERIC
                ELSE NULL
            END
        ) AS Depreciation_on_Fixed_Assets,

         -- ADDED: line item 330 from lineItems JSON
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("lineItems"::TEXT, '')::jsonb ->> '330'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("lineItems"::TEXT, '')::jsonb 
                    ->> '330'
                )::NUMERIC
                ELSE NULL
            END
        ) AS secured_loans,

         -- ADDED: line item 331 from lineItems JSON
    MAX(
            CASE
                WHEN BTRIM(
                    NULLIF("lineItems"::TEXT, '')::jsonb ->> '331'
                ) ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (
                    NULLIF("lineItems"::TEXT, '')::jsonb 
                    ->> '331'
                )::NUMERIC
                ELSE NULL
            END
        ) AS unsecured_loans
    

    FROM {{ source('correlation_readiness', 'ledgerlogs') }}
    GROUP BY ulb_id::TEXT, year::TEXT
),

final_base AS (
SELECT
    b.ulb_name,
    b.ulb_code,
    b.state_name,
    b.iso_code,
    b.financial_year,
    b.population_category,
    b.population_category_sort_order,
    b.population_category_ordered,

    1 AS total_count,

    CASE
        WHEN COALESCE(l.has_standardized_value, FALSE) = TRUE
            THEN 'standardized'

        WHEN COALESCE(l.has_error_value, FALSE) = TRUE
            THEN 'error'

        ELSE 'Ineligible for standardization'
    END AS is_standadized_by_magc,

      CASE
        WHEN COALESCE(l.market_readiness_band, 'N/A') IN (
            'A1 (Highly Prepared)',
            'A2 (Well Prepared)',
            'A3 (Moderately Prepared)'
        )
        THEN 'A'
        ELSE 'Not Market-ready'
    END AS "market ready status",

    l.total_revenue AS "Total Revenue",
    l.revenue_expenditure AS "Revenue Expenditure",
    l.capex AS "CapEx",
    l. Interest_Finance_Charges AS "Interest and Finance Charges",
    l.Depreciation_on_Fixed_Assets AS "Depreciation on Fixed Assets",
    l.secured_loans AS "Secured Loans",
    l.unsecured_loans AS "Unsecured Loans",
    l.market_readiness_band AS "Market Readiness Band",

     (
        COALESCE(l.secured_loans, 0)
        + COALESCE(l.unsecured_loans, 0)
    ) AS "Existing Debt Exposure",

    (
        COALESCE(l.total_revenue, 0)
        - COALESCE(l.revenue_expenditure, 0)
        + COALESCE(l. Interest_Finance_Charges, 0)
        + COALESCE(l.Depreciation_on_Fixed_Assets, 0)
    ) AS operating_surplus,

     CASE
        WHEN (
            COALESCE(l.total_revenue, 0)
            - COALESCE(l.revenue_expenditure, 0)
            + COALESCE(l.interest_finance_charges, 0)
            + COALESCE(l.depreciation_on_fixed_assets, 0)
        ) > 0
        THEN (
            COALESCE(l.total_revenue, 0)
            - COALESCE(l.revenue_expenditure, 0)
            + COALESCE(l.interest_finance_charges, 0)
            + COALESCE(l.depreciation_on_fixed_assets, 0)
        ) * 2.5
        ELSE NULL
    END AS "Debt Carrying Capacity",

     TO_CHAR(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at",

    COALESCE(l.standardization_record_count, 0) AS standardization_record_count

FROM ulb_year_base b
LEFT JOIN standardization_logs l
    ON b.ulb_id::TEXT = l.ulb_id
   AND (
        b.year_id::TEXT = l.year_key
        OR b.financial_year::TEXT = l.year_key
   )
WHERE b.financial_year_start >= 2019
)

SELECT
    *,
    CASE
        WHEN COALESCE("Debt Carrying Capacity", 0) > 0
         AND COALESCE("Existing Debt Exposure", 0) >= 0
        THEN GREATEST(
            COALESCE("Debt Carrying Capacity", 0) - COALESCE("Existing Debt Exposure", 0),
            0
        )
        ELSE 0
    END AS "Additional Debt Mobilization"
FROM final_base