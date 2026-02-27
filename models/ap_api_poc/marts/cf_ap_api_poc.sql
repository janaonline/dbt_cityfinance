{{ config(materialized='table', tags=['ap_api_poc']) }}

/*
  This model builds a flattened, human‑readable table from the
  `cf_ap_api_poc` source.  The raw data comes in as JSON blobs;
  we explode the `lineItems` object into individual rows, look up
  descriptive metadata from the legend table, attach ULB / state /
  year names, classify each row by the “head of account” and record a
  timestamp.

  Materialized as a table for reporting/consumption by downstream
  models or tools.
*/

WITH

-- pull the source collection containing the JSON payloads
datacollections AS (
    SELECT *
    FROM {{ source('cf_ap_api_poc', 'datacollections') }}
),

-- 🔹 inline the lineitemlegends transformation (removed view dependency)
lineitemlegends AS (
    SELECT
        FLOOR("subCode")::int AS subCode,
        FLOOR("majorCode")::int AS majorCode,
        name
    FROM {{ source('cf_ap_api_poc', 'lineitemlegends') }}
),

-- lookup tables for ULBs, states and years
ulbs AS (
    SELECT *
    FROM {{ source('cityfinance_prod', 'ulbs') }}
),

states AS (
    SELECT *
    FROM {{ source('cityfinance_prod', 'states') }}
),

years AS (
    SELECT *
    FROM {{ source('cityfinance_prod', 'years') }}
),

-- 🔹 explode the JSON object in each datacollection row
expanded_lineitems AS (
    SELECT
        dc."_id",                  -- original record id
        dc."ulbId",                -- foreign key to ulb lookup
        dc."yearId",               -- foreign key to year lookup
        li.key::int      AS line_code,   -- numeric code from JSON key
        li.value::numeric AS amount       -- numeric value from JSON
    FROM datacollections dc,
    -- lateral join to turn each key/value pair into its own row
    LATERAL jsonb_each(dc."lineItems"::jsonb) AS li(key, value)
),

-- 🔹 attach descriptive fields and derive additional attributes
final_data AS (
    SELECT
        l.majorCode,                        -- top‑level code from legend
        COALESCE(l.subCode, 0) AS subCode,  -- use zero for missing subcodes
        l.name AS lineItemName,             -- human readable description
        e.amount AS Amount,                 -- monetary amount

        u.name AS ulb,                      -- ULB name
        s.name AS state,                    -- state name
        y.year AS year,                     -- fiscal year

        -- derive head of account based on the pattern of majorCode
        CASE
            WHEN majorCode::text LIKE '1%' THEN 'Income'
            WHEN majorCode::text LIKE '2%' THEN 'Expenditure'
            WHEN majorCode::text LIKE '3%' THEN 'Liability'
            WHEN majorCode::text LIKE '4%' THEN 'Asset'
            ELSE 'Other'
        END AS "headOfAccount",

        -- capture the timestamp in IST, formatted as requested by front end
        to_char(now() AT TIME ZONE 'Asia/Kolkata',
                'FMMonth DD YYYY "at" HH12:MI am') as "updated_at"

    FROM expanded_lineitems e

    -- join to get the legend information; match subCode exactly,
    -- or if subCode is null match on the majorCode alone.
    LEFT JOIN lineitemlegends l
        ON e.line_code = l.subCode
        OR (e.line_code = l.majorCode AND l.subCode IS NULL)

    -- attach ULB, state and year lookups
    LEFT JOIN ulbs u
        ON e."ulbId" = u."_id"

    LEFT JOIN states s
        ON u."state" = s."_id"

    LEFT JOIN years y
        ON e."yearId" = y."_id"
)

-- final result set exposed by this model
SELECT *
FROM final_data