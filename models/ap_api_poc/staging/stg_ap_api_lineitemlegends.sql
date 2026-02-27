{{ config(materialized='view', tags=['ap_api_poc']) }}

SELECT
    FLOOR("subCode")::int AS subCode,
    FLOOR("majorCode")::int AS majorCode,
    name
FROM {{ source('cf_ap_api_poc', 'lineitemlegends') }}