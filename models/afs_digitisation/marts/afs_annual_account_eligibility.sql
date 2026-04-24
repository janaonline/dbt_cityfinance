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
    -- Optional: filter for specific years if needed, e.g., >= 2019
   -- WHERE LEFT(BTRIM(year), 4)::int >= 2019
),

-- Build the base independent structure
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
        -- If isStandardizable is not "No", mark as true
        CASE 
            --WHEN "isStandardizable" <> 'No' THEN true 
            --ELSE false 
            WHEN "isStandardizable" <> 'No' THEN 'Yes' 
            ELSE 'No'
        END AS is_standadized_by_magc
    FROM {{ source('afs_digitisation', 'ledgerlogs') }}
)


SELECT
    b.ulb_name,
    b.ulb_code,
    b.state_name,
    b.iso_code,
    b.financial_year,
    CASE 
        -- Logic for Financial Year 2023-24 and onwards
        WHEN b.financial_year >= '2023-24' THEN
            CASE 
                WHEN a."currentFormStatus" IN (4, 6) THEN 'eligible'
                WHEN a."currentFormStatus" IN (3, 4, 6) THEN 'submitted'
                ELSE 'ineligible'
            END
            
        -- Logic for Financial Year before 2023-24
        ELSE
            CASE 
                WHEN a.status = 'APPROVED' AND a."actionTakenByRole" IN ('MoHUA', 'STATE') 
                    THEN 'eligible'
                WHEN (a.status IN ('APPROVED', 'PENDING')) AND a."actionTakenByRole" IN ('MoHUA', 'STATE') 
                    THEN 'submitted'
                ELSE 'ineligible'
            END
    END AS status,

    --COALESCE(l.is_standadized_by_magc, false) AS is_standadized_by_magc,
    COALESCE(l.is_standadized_by_magc, 'No') AS is_standadized_by_magc,
    
    -- Added a helper column to see if a record even exists in the source
    CASE WHEN a.ulb IS NULL THEN 0 ELSE 1 END AS has_annual_account_record

FROM ulb_year_base b
LEFT JOIN annual_accounts a 
    ON b.ulb_id = a.ulb 
    AND b.year_id = a.design_year
LEFT JOIN standardization_logs l 
    ON b.ulb_id = l.ulb_id 
    AND b.financial_year = l.year