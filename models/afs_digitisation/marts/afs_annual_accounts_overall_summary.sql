{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH base AS (
    SELECT *
    FROM {{ ref('afs_annual_account_eligibility') }}
),

shifted AS (
    SELECT
        ulb_name,
        ulb_code,
        state_name,
        iso_code,

        LEFT(BTRIM(financial_year), 4)::int - 2 AS shifted_fy_start_year,

        status,
        is_standadized_by_magc,
        has_annual_account_record
    FROM base
    WHERE BTRIM(financial_year) ~ '^[0-9]{4}-[0-9]{2}$'
)

SELECT
    ulb_name,
    ulb_code,
    state_name,
    iso_code,

    shifted_fy_start_year::text
        || '-' ||
    RIGHT((shifted_fy_start_year + 1)::text, 2) AS financial_year,

    status,
    is_standadized_by_magc,
    has_annual_account_record

FROM shifted
WHERE shifted_fy_start_year >= 2019