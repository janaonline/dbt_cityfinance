{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH source_data AS (
    SELECT DISTINCT
        ulb_name, ulb_code, state_name, iso_code, financial_year, doc_type, audit_type, 
        digitization_status, digitization_pages, population_category, population_category_sort_order,
        population_category_ordered, uploaded_by, file_processed_on_detailed, file_processed_on_month_year, processed_time_ago
    FROM {{ ref('afs_ocr_xl_files_dump') }}
    WHERE BTRIM(COALESCE(financial_year, '')) ~ '^[0-9]{4}-[0-9]{2}$'
      AND LEFT(BTRIM(financial_year), 4)::int >= 2019
      AND COALESCE(BTRIM(doc_type), '') <> ''
),

normalized AS (
    SELECT 
        *,
        CASE
            WHEN REGEXP_REPLACE(LOWER(BTRIM(COALESCE(audit_type, ''))), '\s+', '', 'g') = 'audited' THEN 'audited'
            WHEN REGEXP_REPLACE(LOWER(BTRIM(COALESCE(audit_type, ''))), '\s+', '', 'g') = 'unaudited' THEN 'unaudited'
            ELSE NULL
        END AS audit_type_norm,
        CASE
            WHEN LOWER(BTRIM(COALESCE(digitization_status, ''))) LIKE '%digit%' THEN 'digitized'
            WHEN LOWER(BTRIM(COALESCE(digitization_status, ''))) LIKE '%fail%' THEN 'failed'
            ELSE NULL
        END AS digitization_status_norm
    FROM source_data
),

scored AS (
    SELECT 
        *,
        CASE
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'digitized' THEN 1
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'digitized' THEN 2
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'failed'    THEN 3
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'failed'    THEN 4
            WHEN audit_type_norm = 'audited'                                          THEN 5
            WHEN audit_type_norm = 'unaudited'                                        THEN 6
            ELSE 7
        END AS preference_rank,
        CASE
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'digitized' THEN 'kept_audited_digitized'
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'digitized' THEN 'kept_unaudited_digitized_because_audited_better_row_not_found'
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'failed'    THEN 'kept_audited_failed'
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'failed'    THEN 'kept_unaudited_failed'
            WHEN audit_type_norm = 'audited'   THEN 'kept_audited_other_status'
            WHEN audit_type_norm = 'unaudited' THEN 'kept_unaudited_other_status'
            ELSE 'kept_unknown_case'
        END AS selection_reason
    FROM normalized
),

deduped AS (
    SELECT *,
        COUNT(*) OVER (PARTITION BY ulb_code, financial_year, doc_type) AS duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY ulb_code, financial_year, doc_type
            ORDER BY preference_rank, CASE WHEN digitization_pages IS NULL THEN 1 ELSE 0 END, digitization_pages DESC
        ) AS rn
    FROM scored
),

annual_accounts_status AS (
    SELECT
        ulb_code,
        financial_year,
        MAX(status) AS annual_account_status
    FROM {{ ref('afs_annual_account_eligibility') }}
    WHERE BTRIM(COALESCE(financial_year, '')) ~ '^[0-9]{4}-[0-9]{2}$'
      AND COALESCE(BTRIM(ulb_code), '') <> ''
    GROUP BY
        ulb_code,
        financial_year
)


SELECT
    d.ulb_name,
    d.ulb_code,
    d.state_name,
    d.iso_code,
    d.financial_year,
    d.doc_type,
    d.audit_type,
    d.digitization_status,
    d.digitization_pages,
    d.population_category,
    d.population_category_sort_order,
    d.population_category_ordered,

    aas.annual_account_status,

    d.selection_reason,
    d.duplicate_count,
    d.uploaded_by,
    d.file_processed_on_detailed,
    d.file_processed_on_month_year,
    d.processed_time_ago,

    -- Static row count for Superset metrics
    1 AS total_count,
    -- Status check count
    CASE WHEN digitization_status IS NULL THEN 0 ELSE 1 END AS status_check_count,
    -- Run-time timestamp for the dashboard
    TO_CHAR(now() AT TIME ZONE 'Asia/Kolkata', 'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"

FROM deduped d
LEFT JOIN annual_accounts_status aas
    ON d.ulb_code = aas.ulb_code
   AND d.financial_year = aas.financial_year
WHERE d.rn = 1