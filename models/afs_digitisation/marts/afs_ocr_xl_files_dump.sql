{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

{%- set columns = adapter.get_columns_in_relation(source('afs_digitisation', 'afs_xl_files')) -%}
{%- set column_names = columns | map(attribute='name') | list -%}

WITH raw_afs AS (
    SELECT 
        *, 
        "auditType" AS audit_type,
        -- Check if afsFile exists; if not, return NULL cast as jsonb
        {% if 'afsFile' in column_names -%}
        "afsFile"::jsonb as afs_json,
        {%- else -%}
        NULL::jsonb as afs_json,
        {%- endif %}
        
        -- Check if ulbFile exists; if not, return NULL cast as jsonb
        {% if 'ulbFile' in column_names -%}
        "ulbFile"::jsonb as ulb_json
        {%- else -%}
        NULL::jsonb as ulb_json
        {%- endif %}
    FROM {{ source('afs_digitisation', 'afs_xl_files') }}
),

processed_afs AS (
    SELECT
        ulb,
        year,
        "docType" AS doc_type,
        audit_type,
        CASE 
            WHEN afs_json IS NOT NULL THEN afs_json ->> 'digitizationStatus'
            ELSE ulb_json ->> 'digitizationStatus'
        END AS digitization_status,
        -- Logic for uploaded_by
        CASE 
            WHEN afs_json IS NOT NULL THEN 'afs'
            ELSE 'ulb'
        END AS uploaded_by,
        CASE 
            WHEN afs_json IS NOT NULL THEN afs_json ->> 'noOfPages'
            ELSE ulb_json ->> 'noOfPages'
        END AS raw_pages,
        
        -- Detailed Timestamp (Full Date & Time)
        TO_CHAR(
            (CASE 
                WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz
                ELSE (ulb_json ->> 'createdAt')::timestamptz
            END) AT TIME ZONE 'Asia/Kolkata', 
            'FMMonth DD YYYY "at" HH12:MI am'
        ) AS file_processed_on_detailed,

        -- Month & Year Only for easier aggregation
        TO_CHAR(
            (CASE 
                WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz
                ELSE (ulb_json ->> 'createdAt')::timestamptz
            END) AT TIME ZONE 'Asia/Kolkata', 
            'FMMonth YYYY'
        ) AS file_processed_on_month_year, 

        -- Requested "Time Ago" Logic
        CASE
            WHEN (now() - (CASE WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz ELSE (ulb_json ->> 'createdAt')::timestamptz END)) < interval '1 day' 
                THEN 'today'
            WHEN (now() - (CASE WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz ELSE (ulb_json ->> 'createdAt')::timestamptz END)) < interval '2 days' 
                THEN 'day ago'
            WHEN (now() - (CASE WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz ELSE (ulb_json ->> 'createdAt')::timestamptz END)) < interval '8 days' 
                THEN 'week ago'
            WHEN (now() - (CASE WHEN afs_json IS NOT NULL THEN (afs_json ->> 'createdAt')::timestamptz ELSE (ulb_json ->> 'createdAt')::timestamptz END)) < interval '31 days' 
                THEN 'month ago'
            ELSE 'More than a month ago'
        END AS processed_time_ago

    FROM raw_afs
),

states AS (
    SELECT _id, name 
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

-- Source all dimension tables with proper filtering
ulbs AS (
    SELECT 
        u._id, 
        u.name, 
        u.state, 
        u.code,
        CASE
            WHEN u.population < 100000 THEN '<100K'
            WHEN u.population < 500000 THEN '100K-500K'
            WHEN u.population < 1000000 THEN '500K-1M'
            WHEN u.population < 4000000 THEN '1M-4M'
            WHEN u.population >= 4000000 THEN '4M+'
            ELSE 'NA'
        END AS population_category
    FROM {{ source('cityfinance_prod', 'ulbs') }} u
    INNER JOIN states s ON u.state = s._id  -- This filters out ULBs from UTs
    WHERE u."isActive" = 'true' 
      AND u."isPublish" = 'true'
),

years AS (
    SELECT _id, year 
    FROM {{ source('cityfinance_prod', 'years') }}
),

-- NEW: Define the specific document types required for each ULB/Year
doc_types AS (
    SELECT 'bal_sheet' AS doc_type
    UNION ALL SELECT 'inc_exp_schedules'
    UNION ALL SELECT 'cash_flow'
    UNION ALL SELECT 'bal_sheet_schedules'
    UNION ALL SELECT 'inc_exp'
),

-- UPDATED: Matrix now includes every ULB × every Year × every Doc Type
ulb_year_matrix AS (
    SELECT
        u._id AS ulb_id,
        u.name AS ulb_name,
        u.code AS ulb_code,
        u.state AS state_id,
        u.population_category,
        y._id AS year_id,
        y.year,
        d.doc_type
    FROM ulbs u
    CROSS JOIN years y
    CROSS JOIN doc_types d
)

SELECT
    m.ulb_name,
    m.ulb_code,
    s.name AS state_name,
    m.year AS Financial_Year,
    CASE 
        WHEN m.doc_type = 'bal_sheet'           THEN 'Balance Sheet'
        WHEN m.doc_type = 'inc_exp'             THEN 'Income & Expenditure'
        WHEN m.doc_type = 'cash_flow'           THEN 'Cash Flow'
        WHEN m.doc_type = 'bal_sheet_schedules' THEN 'Balance Sheet Schedules'
        WHEN m.doc_type = 'inc_exp_schedules'   THEN 'Income & Expenditure Schedules'
        ELSE m.doc_type 
    END AS doc_type,
    p.audit_type,
    p.uploaded_by,
    p.file_processed_on_detailed,
    p.file_processed_on_month_year,
    p.processed_time_ago,
    BTRIM(p.digitization_status) AS digitization_status,
    CASE 
        WHEN BTRIM(p.raw_pages) ~ '^[0-9]+$' THEN BTRIM(p.raw_pages)::int 
        ELSE NULL 
    END AS digitization_pages,
    m.population_category,

    to_char(now() AT TIME ZONE 'Asia/Kolkata',
                'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"
FROM
    ulb_year_matrix m
    LEFT JOIN processed_afs p 
        ON m.ulb_id = p.ulb 
        AND m.year_id = p.year 
        AND m.doc_type = p.doc_type
    LEFT JOIN states s ON m.state_id = s._id
ORDER BY s.name, m.ulb_name, m.year, m.doc_type