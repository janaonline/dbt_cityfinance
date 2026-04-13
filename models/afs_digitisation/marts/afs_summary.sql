-- Configuration: Materialize as a table for improved query performance
-- Tagged with 'ap_api_poc' for easy identification and selective execution
{{ config(materialized='table', tags=['afs_digitisation']) }}


afsxlfiles AS (
    SELECT *
    FROM {{ source('afs_digitisation', 'afs_xl_files') }}
),


states AS (
    SELECT *
    FROM {{ source('cityfinance_prod', 'states') }}
),