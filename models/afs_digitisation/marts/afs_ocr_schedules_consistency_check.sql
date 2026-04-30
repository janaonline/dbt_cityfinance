{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH base_data AS (
    SELECT 
        ulb_name, 
        ulb_code, 
        state_name, 
        iso_code,
        financial_year,
        audit_type, 
        population_category, 
        population_category_sort_order,
        population_category_ordered,
        file_processed_on_detailed, 
        file_processed_on_month_year, 
        processed_time_ago,
        doc_type,
        digitization_status,
        annual_account_status
    FROM {{ ref('afs_ocr_summary') }}
),

-- 1. Pivot the data to a single row per ULB + Year
ulb_year_summary AS (
    SELECT
        ulb_name,
        ulb_code,
        state_name,
        iso_code,
        financial_year,
        population_category,
        population_category_sort_order,
        population_category_ordered,

        -- Annual account status from afs_ocr_summary
        MAX(annual_account_status) AS annual_accounts_status,

        -- We take the most recent processing info available for the ULB/Year
        MAX(file_processed_on_detailed) as file_processed_on_detailed,
        MAX(file_processed_on_month_year) as file_processed_on_month_year,
        MAX(processed_time_ago) as processed_time_ago,
        
        -- Pulling specific values for the two schedules to compare
        MAX(CASE WHEN doc_type = 'Balance Sheet Schedules' THEN digitization_status END) as bs_status,
        MAX(CASE WHEN doc_type = 'Balance Sheet Schedules' THEN audit_type END) as bs_audit,
        MAX(CASE WHEN doc_type = 'Income & Expenditure Schedules' THEN digitization_status END) as ie_status,
        MAX(CASE WHEN doc_type = 'Income & Expenditure Schedules' THEN audit_type END) as ie_audit
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

-- 2. Apply the single-row logic and consistency check
SELECT
    ulb_name, 
    ulb_code, 
    state_name, 
    iso_code,
    financial_year,
    population_category, 
    population_category_sort_order,
    population_category_ordered,
    annual_accounts_status,
    file_processed_on_detailed, 
    file_processed_on_month_year, 
    processed_time_ago,
    1 AS total_count,
    -- The consistency check logic
    CASE 
        WHEN bs_status = 'digitized' AND ie_status = 'digitized'
             AND bs_audit IS NOT NULL AND ie_audit IS NOT NULL
             AND bs_audit = ie_audit
        THEN 1 
        ELSE 0 
    END AS is_schedules_digitized_consistent,
    -- Metadata for the dashboard to show current audit type context
    COALESCE(bs_audit, ie_audit) as primary_audit_type,
    -- Run-time timestamp
    to_char(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
FROM ulb_year_summary