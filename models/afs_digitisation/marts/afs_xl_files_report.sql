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
        state
    FROM {{ source('cityfinance_prod', 'ulbs') }}
),

states AS (
    SELECT
        _id,
        name
    FROM {{ source('cityfinance_prod', 'states') }}
)

SELECT
    --afs.*,

    u.name AS ulb_name,
    s.name AS state_name

FROM afs
LEFT JOIN ulbs u
    ON afs.ulb = u._id

LEFT JOIN states s
    ON u.state = s._id