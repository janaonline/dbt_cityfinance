-- Configuration: Materialize as a table for improved query performance
-- Tagged with 'ap_api_poc' for easy identification and selective execution
{{ config(materialized='table', tags=['afs_digitisation']) }}

WITH
afs AS (
    SELECT *
    FROM {{ source('afs_digitisation', 'afs_xl_files') }}
),

ulbs AS (
    SELECT
        _id,
        name,
        state,
        population,
        code
    FROM {{ source('cityfinance_prod', 'ulbs') }}
),

states AS (
    SELECT
        _id,
        name
    FROM {{ source('cityfinance_prod', 'states') }}
),

years AS (
    SELECT
        _id,
        year
    FROM {{ source('cityfinance_prod', 'years') }}
)

SELECT
    --afs.*,

    u.name AS ulb_name,
    u.code AS ulb_code,
    s.name AS state_name,
    y.year AS Financial_Year,
    afs."docType" AS doc_type,

     CASE
    WHEN NULLIF(BTRIM(to_jsonb(afs) -> 'afsFile' ->> 'digitizationStatus'), '') IS NOT NULL
    THEN to_jsonb(afs) -> 'afsFile' ->> 'digitizationStatus'
    ELSE to_jsonb(afs) -> 'ulbFile' ->> 'digitizationStatus'
END AS digitization_status,

CASE
    WHEN NULLIF(BTRIM(to_jsonb(afs) -> 'afsFile' ->> 'digitizationStatus'), '') IS NOT NULL
    THEN
        CASE
            WHEN NULLIF(BTRIM(to_jsonb(afs) -> 'afsFile' ->> 'noOfPages'), '') ~ '^[0-9]+$'
            THEN (to_jsonb(afs) -> 'afsFile' ->> 'noOfPages')::int
            ELSE NULL
        END
    ELSE
        CASE
            WHEN NULLIF(BTRIM(to_jsonb(afs) -> 'ulbFile' ->> 'noOfPages'), '') ~ '^[0-9]+$'
            THEN (to_jsonb(afs) -> 'ulbFile' ->> 'noOfPages')::int
            ELSE NULL
        END
END AS digitization_pages,

        CASE
        WHEN u.population < 100000 THEN '<100K'
        WHEN u.population >= 100000 AND u.population < 500000 THEN '100K-500K'
        WHEN u.population >= 500000 AND u.population < 1000000 THEN '500K-1M'
        WHEN u.population >= 1000000 AND u.population < 4000000 THEN '1M-4M'
        WHEN u.population >= 4000000 THEN '4M+'
        ELSE 'NA'
    END AS population_category

FROM afs
LEFT JOIN ulbs u
    ON afs.ulb = u._id 

LEFT JOIN years y
    ON afs.year = y._id

LEFT JOIN states s
    ON u.state = s._id