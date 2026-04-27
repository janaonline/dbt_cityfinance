{{ config(materialized = 'table', tags = ['afs_digitisation']) }}

WITH states AS (
    SELECT _id, name 
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

ulbs AS (
    SELECT 
        u._id AS ulb_id, 
        u.name AS ulb_name, 
        u.state AS state_id, 
        u.code AS ulb_code,
        CASE
            WHEN u.population < 100000 THEN '<100K'
            WHEN u.population < 500000 THEN '100K-500K'
            WHEN u.population < 1000000 THEN '500K-1M'
            WHEN u.population < 4000000 THEN '1M-4M'
            WHEN u.population >= 4000000 THEN '4M+'
            ELSE 'NA'
        END AS population_category
    FROM {{ source('cityfinance_prod', 'ulbs') }} u
    INNER JOIN states s ON u.state = s._id
    WHERE u."isActive" = 'true' 
      AND u."isPublish" = 'true'
),

iso_codes AS (
    SELECT state, iso_code
    FROM {{ source('cityfinance_prod','iso_codes') }}
),

years AS (
    SELECT _id AS year_id, year AS financial_year 
    FROM {{ source('cityfinance_prod', 'years') }}
),

ulb_year_base AS (
    SELECT
        u.ulb_id,
        u.ulb_name,
        u.ulb_code,
        s.name AS state_name,
        i.iso_code,
        u.population_category,
        y.year_id,
        y.financial_year
    FROM ulbs u
    CROSS JOIN years y
    LEFT JOIN states s ON u.state_id = s._id
    LEFT JOIN iso_codes i ON s.name = i.state
),

annual_accounts AS (
    SELECT 
        ulb,
        design_year,
        "currentFormStatus",
        status,
        "actionTakenByRole"
    FROM {{ source('cityfinance_prod', 'annualaccountdatas') }}
),

standardization_logs AS (
    SELECT 
        ulb_id,
        year,
        NULLIF(BTRIM("isStandardizable"), '') AS is_standardizable
    FROM {{ source('afs_digitisation', 'ledgerlogs') }}
),

annual_account_status AS (
    SELECT
        b.ulb_name,
        b.ulb_code,
        b.state_name,
        b.iso_code,
        b.financial_year,
        b.ulb_id,
        b.year_id,

        CASE 
            WHEN b.financial_year >= '2023-24' THEN
                CASE 
                    WHEN a."currentFormStatus" IN (4, 6) THEN 'eligible'
                    WHEN a."currentFormStatus" IN (3, 4, 6) THEN 'submitted'
                    ELSE 'ineligible'
                END
            ELSE
                CASE 
                    WHEN a.status = 'APPROVED' 
                         AND a."actionTakenByRole" IN ('MoHUA', 'STATE') 
                        THEN 'eligible'

                    WHEN a.status IN ('APPROVED', 'PENDING') 
                         AND a."actionTakenByRole" IN ('MoHUA', 'STATE') 
                        THEN 'submitted'

                    ELSE 'ineligible'
                END
        END AS status,

        CASE 
            WHEN a.ulb IS NULL THEN 0 
            ELSE 1 
        END AS has_annual_account_record

    FROM ulb_year_base b
    LEFT JOIN annual_accounts a 
        ON b.ulb_id = a.ulb 
       AND b.year_id = a.design_year
)

SELECT
    aas.ulb_name,
    aas.ulb_code,
    aas.state_name,
    aas.iso_code,
    aas.financial_year,
    aas.status,

    CASE
        WHEN l.is_standardizable IS NOT NULL 
             AND LOWER(l.is_standardizable) <> 'no'
            THEN 'standardized'

        WHEN LOWER(l.is_standardizable) = 'no'
            THEN 'error'

        WHEN l.is_standardizable IS NULL 
             AND aas.status = 'eligible'
            THEN 'yet to standardized'

        ELSE NULL
    END AS is_standadized_by_magc,

    aas.has_annual_account_record

FROM annual_account_status aas
LEFT JOIN standardization_logs l 
    ON aas.ulb_id = l.ulb_id 
   AND aas.financial_year = l.year