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
        CASE 
            WHEN afs_json IS NOT NULL THEN afs_json ->> 'noOfPages'
            ELSE ulb_json ->> 'noOfPages'
        END AS raw_pages
    FROM raw_afs
),

-- Source all dimension tables with proper filtering
ulbs AS (
    SELECT _id, name, state, code,
        CASE
            WHEN population < 100000 THEN '<100K'
            WHEN population < 500000 THEN '100K-500K'
            WHEN population < 1000000 THEN '500K-1M'
            WHEN population < 4000000 THEN '1M-4M'
            WHEN population >= 4000000 THEN '4M+'
            ELSE 'NA'
        END AS population_category
    FROM {{ source('cityfinance_prod', 'ulbs') }}
    WHERE "isActive" = 'true' 
      AND "isPublish" = 'true'
),

states AS (
    SELECT _id, name 
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

years AS (
    SELECT _id, year 
    FROM {{ source('cityfinance_prod', 'years') }}
),

-- Create matrix of all active ULBs × all years
ulb_year_matrix AS (
    SELECT
        u._id AS ulb_id,
        u.name AS ulb_name,
        u.code AS ulb_code,
        u.state AS state_id,
        u.population_category,
        y._id AS year_id,
        y.year
    FROM ulbs u
    CROSS JOIN years y
)

SELECT
    m.ulb_name,
    m.ulb_code,
    s.name AS state_name,
    m.year AS Financial_Year,
    p.doc_type,
    p.audit_type,
    BTRIM(p.digitization_status) AS digitization_status,
    CASE 
        WHEN BTRIM(p.raw_pages) ~ '^[0-9]+$' THEN BTRIM(p.raw_pages)::int 
        ELSE NULL 
    END AS digitization_pages,
    m.population_category
FROM
    ulb_year_matrix m
    LEFT JOIN processed_afs p ON m.ulb_id = p.ulb AND m.year_id = p.year
    LEFT JOIN states s ON m.state_id = s._id
ORDER BY s.name, m.ulb_name, m.year