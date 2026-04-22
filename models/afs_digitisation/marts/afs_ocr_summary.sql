{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH source_data AS (
    SELECT DISTINCT
        ulb_name,
        ulb_code,
        state_name,
        financial_year,
        doc_type,
        audit_type,
        digitization_status,
        digitization_pages,
        population_category
    FROM {{ ref('afs_xl_files_report') }}
     WHERE BTRIM(COALESCE(financial_year, '')) ~ '^[0-9]{4}-[0-9]{2}$'
      AND LEFT(BTRIM(financial_year), 4)::int >= 2019
      AND COALESCE(BTRIM(doc_type), '') <> ''
),

normalized AS (
    SELECT
        ulb_name,
        ulb_code,
        state_name,
        financial_year,
        doc_type,
        audit_type,
        BTRIM(digitization_status) AS digitization_status,
        digitization_pages,
        population_category,

        CASE
            WHEN REGEXP_REPLACE(LOWER(BTRIM(COALESCE(audit_type, ''))), '\s+', '', 'g') = 'audited'
                THEN 'audited'
            WHEN REGEXP_REPLACE(LOWER(BTRIM(COALESCE(audit_type, ''))), '\s+', '', 'g') = 'unaudited'
                THEN 'unaudited'
            ELSE NULL
        END AS audit_type_norm,

        CASE
            WHEN LOWER(BTRIM(COALESCE(digitization_status, ''))) LIKE '%digit%'
                THEN 'digitized'
            WHEN LOWER(BTRIM(COALESCE(digitization_status, ''))) LIKE '%fail%'
                THEN 'failed'
            ELSE NULL
        END AS digitization_status_norm
    FROM source_data
),

scored AS (
    SELECT
        ulb_name,
        ulb_code,
        state_name,
        financial_year,
        doc_type,
        audit_type,
        digitization_status,
        digitization_pages,
        population_category,
        audit_type_norm,
        digitization_status_norm,

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
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'digitized'
                THEN 'kept_audited_digitized'
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'digitized'
                THEN 'kept_unaudited_digitized_because_audited_better_row_not_found'
            WHEN audit_type_norm = 'audited'   AND digitization_status_norm = 'failed'
                THEN 'kept_audited_failed'
            WHEN audit_type_norm = 'unaudited' AND digitization_status_norm = 'failed'
                THEN 'kept_unaudited_failed'
            WHEN audit_type_norm = 'audited'
                THEN 'kept_audited_other_status'
            WHEN audit_type_norm = 'unaudited'
                THEN 'kept_unaudited_other_status'
            ELSE 'kept_unknown_case'
        END AS selection_reason
    FROM normalized
),

deduped AS (
    SELECT
        ulb_name,
        ulb_code,
        state_name,
        financial_year,
        doc_type,
        audit_type,
        digitization_status,
        digitization_pages,
        population_category,
        audit_type_norm,
        digitization_status_norm,
        preference_rank,
        selection_reason,
        COUNT(*) OVER (
            PARTITION BY ulb_code, financial_year, doc_type
        ) AS duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY ulb_code, financial_year, doc_type
            ORDER BY
                preference_rank,
                CASE WHEN digitization_pages IS NULL THEN 1 ELSE 0 END,
                digitization_pages DESC,
                audit_type,
                digitization_status,
                ulb_name,
                state_name
        ) AS rn
    FROM scored
)

SELECT
    ulb_name,
    ulb_code,
    state_name,
    financial_year,
    doc_type,
    audit_type,
    digitization_status,
    digitization_pages,
    population_category,
    selection_reason,
    duplicate_count
FROM deduped
WHERE rn = 1