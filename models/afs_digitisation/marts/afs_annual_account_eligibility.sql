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
        END AS population_category,

        -- ADDED: numeric sort order for Superset
        CASE
            WHEN u.population >= 4000000 THEN 1
            WHEN u.population >= 1000000 THEN 2
            WHEN u.population >= 500000 THEN 3
            WHEN u.population >= 100000 THEN 4
            WHEN u.population < 100000 THEN 5
            ELSE 99
        END AS population_category_sort_order,

        -- ADDED: fallback ordered label for Superset if numeric sorting does not work
        CASE
            WHEN u.population >= 4000000 THEN '01 - 4M+'
            WHEN u.population >= 1000000 THEN '02 - 1M-4M'
            WHEN u.population >= 500000 THEN '03 - 500K-1M'
            WHEN u.population >= 100000 THEN '04 - 100K-500K'
            WHEN u.population < 100000 THEN '05 - <100K'
            ELSE '99 - NA'
        END AS population_category_ordered
        
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
    SELECT 
        _id AS year_id,
        year AS financial_year,
        CAST(SPLIT_PART(year, '-', 1) AS INTEGER) AS financial_year_start
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
        u.population_category_sort_order,
        u.population_category_ordered,
        y.year_id,
        y.financial_year,
        y.financial_year_start
    FROM ulbs u
    CROSS JOIN years y
    LEFT JOIN states s ON u.state_id = s._id
    LEFT JOIN iso_codes i ON s.name = i.state
),

annual_accounts_raw AS (
    SELECT 
        ulb AS ulb_id,

        NULLIF(BTRIM(status), '') AS top_level_status,
        NULLIF(BTRIM("actionTakenByRole"), '') AS action_taken_by_role,
        NULLIF(BTRIM("isDraft"), '') AS is_draft,
        NULLIF(BTRIM(audited::JSONB ->> 'submit_annual_accounts'), '') AS audited_submit_annual_accounts,
        NULLIF(BTRIM("unAudited"::JSONB ->> 'submit_annual_accounts'), '') AS unaudited_submit_annual_accounts,
        
         (
            NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,bal_sheet,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,bal_sheet_schedules,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,inc_exp,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,inc_exp_schedules,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,cash_flow,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM(audited::JSONB #>> '{provisional_data,auditor_report,pdf,url}')), 'null'), '') IS NOT NULL
        ) AS audited_has_required_pdf_urls,

        
        (
            NULLIF(NULLIF(LOWER(BTRIM("unAudited"::JSONB #>> '{provisional_data,bal_sheet,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM("unAudited"::JSONB #>> '{provisional_data,bal_sheet_schedules,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM("unAudited"::JSONB #>> '{provisional_data,inc_exp,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM("unAudited"::JSONB #>> '{provisional_data,inc_exp_schedules,pdf,url}')), 'null'), '') IS NOT NULL
            AND NULLIF(NULLIF(LOWER(BTRIM("unAudited"::JSONB #>> '{provisional_data,cash_flow,pdf,url}')), 'null'), '') IS NOT NULL
        ) AS unaudited_has_required_pdf_urls,


        CASE
            WHEN NULLIF(BTRIM("currentFormStatus"::TEXT), '') ~ '^[0-9]+(\.[0-9]+)?$'
                THEN NULLIF(BTRIM("currentFormStatus"::TEXT), '')::NUMERIC
            ELSE NULL
        END AS current_form_status,

        COALESCE(
            audited::JSONB -> 'year' ->> '$oid',
            audited::JSONB ->> 'year'
        ) AS audited_year_id,

        NULLIF(
            BTRIM(
                COALESCE(
                    audited::JSONB ->> 'status',
                    status
                )
            ),
            ''
        ) AS audited_raw_status,

        COALESCE(
            "unAudited"::JSONB -> 'year' ->> '$oid',
            "unAudited"::JSONB ->> 'year'
        ) AS unaudited_year_id,

        NULLIF(
            BTRIM(
                COALESCE(
                    "unAudited"::JSONB ->> 'status',
                    status
                )
            ),
            ''
        ) AS unaudited_raw_status

    FROM {{ source('cityfinance_prod', 'annualaccountdatas') }}
),

audited_records AS (
    SELECT
        aar.ulb_id,
        y.year_id,

          CASE
          
            WHEN y.financial_year_start <= 2020 THEN
                CASE

                 -- First gate: audited required PDFs only
                    WHEN COALESCE(aar.audited_has_required_pdf_urls, FALSE) = FALSE
                        THEN 'ineligible'
                   WHEN UPPER(aar.action_taken_by_role) = 'ULB'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.audited_submit_annual_accounts) = 'TRUE'
                        --  AND UPPER(aar.unaudited_submit_annual_accounts) = 'TRUE'
                        THEN 'submitted'

                    WHEN UPPER(aar.action_taken_by_role) = 'STATE'
                         AND UPPER(aar.is_draft) = 'TRUE'
                        THEN 'eligible'

                    WHEN UPPER(aar.action_taken_by_role) = 'STATE'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.audited_raw_status) = 'APPROVED'
                        THEN 'eligible'

                    WHEN UPPER(aar.action_taken_by_role) = 'MOHUA'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.audited_raw_status) = 'APPROVED'
                        THEN 'eligible'

                    ELSE 'ineligible'
                END

            WHEN y.financial_year_start >= 2021 THEN
                CASE
                 -- First gate: audited required PDFs only
                    WHEN COALESCE(aar.audited_has_required_pdf_urls, FALSE) = FALSE
                        THEN 'ineligible'
                    WHEN aar.current_form_status IN (3, 4, 6)
                         AND UPPER(aar.audited_submit_annual_accounts) = 'TRUE'
                        THEN 'eligible'
                    ELSE 'ineligible'
                END

            ELSE 'ineligible'
        END AS audited_status

    FROM annual_accounts_raw aar
    INNER JOIN years y 
        ON aar.audited_year_id = y.year_id
    WHERE aar.audited_year_id IS NOT NULL
),

audited_status_by_ulb_year AS (
    SELECT
        ulb_id,
        year_id,

        CASE
            WHEN BOOL_OR(audited_status = 'eligible') THEN 'eligible'
            WHEN BOOL_OR(audited_status = 'submitted') THEN 'submitted'
            ELSE 'ineligible'
        END AS audited_status,

        1 AS has_audited_record

    FROM audited_records
    GROUP BY ulb_id, year_id
),

unaudited_records AS (
    SELECT
        aar.ulb_id,
        y.year_id,

        CASE
           
            WHEN y.financial_year_start <= 2021 THEN
                 CASE
                 -- First gate: unaudited required PDFs only
                    WHEN COALESCE(aar.unaudited_has_required_pdf_urls, FALSE) = FALSE
                        THEN 'ineligible'
                    WHEN UPPER(aar.action_taken_by_role) = 'ULB'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.unaudited_submit_annual_accounts) = 'TRUE'
                        --  AND UPPER(aar.audited_submit_annual_accounts) = 'TRUE'
                        THEN 'submitted'

                    WHEN UPPER(aar.action_taken_by_role) = 'STATE'
                         AND UPPER(aar.is_draft) = 'TRUE'
                        THEN 'eligible'

                    WHEN UPPER(aar.action_taken_by_role) = 'STATE'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.unaudited_raw_status) = 'APPROVED'
                        THEN 'eligible'

                    WHEN UPPER(aar.action_taken_by_role) = 'MOHUA'
                         AND UPPER(aar.is_draft) = 'FALSE'
                         AND UPPER(aar.unaudited_raw_status) = 'APPROVED'
                        THEN 'eligible'

                    ELSE 'ineligible'
                END

            WHEN y.financial_year_start >= 2022 THEN
                CASE
                -- First gate: unaudited required PDFs only
                    WHEN COALESCE(aar.unaudited_has_required_pdf_urls, FALSE) = FALSE
                        THEN 'ineligible'
                    WHEN aar.current_form_status IN (3, 4, 6)
                         AND UPPER(aar.unaudited_submit_annual_accounts) = 'TRUE'
                        THEN 'eligible'
                    ELSE 'ineligible'
                END

            ELSE 'ineligible'
        END AS unaudited_status

    FROM annual_accounts_raw aar
    INNER JOIN years y 
        ON aar.unaudited_year_id = y.year_id
    WHERE aar.unaudited_year_id IS NOT NULL
),

unaudited_status_by_ulb_year AS (
    SELECT
        ulb_id,
        year_id,

        CASE
            WHEN BOOL_OR(unaudited_status = 'eligible') THEN 'eligible'
            WHEN BOOL_OR(unaudited_status = 'submitted') THEN 'submitted'
            ELSE 'ineligible'
        END AS unaudited_status,

        1 AS has_unaudited_record

    FROM unaudited_records
    GROUP BY ulb_id, year_id
),

annual_account_status AS (
    SELECT
        b.ulb_name,
        b.ulb_code,
        b.state_name,
        b.iso_code,
        b.population_category,
        b.population_category_sort_order,
        b.population_category_ordered,
        b.financial_year,
        b.ulb_id,
        b.year_id,

        COALESCE(a.audited_status, 'ineligible') AS audited_status,
        COALESCE(u.unaudited_status, 'ineligible') AS unaudited_status,

        CASE 
            WHEN COALESCE(a.has_audited_record, 0) = 1 
              OR COALESCE(u.has_unaudited_record, 0) = 1 
                THEN 1 
            ELSE 0 
        END AS has_annual_account_record

    FROM ulb_year_base b
    LEFT JOIN audited_status_by_ulb_year a
        ON b.ulb_id = a.ulb_id
       AND b.year_id = a.year_id
    LEFT JOIN unaudited_status_by_ulb_year u
        ON b.ulb_id = u.ulb_id
       AND b.year_id = u.year_id
),

standardization_logs AS (
    SELECT 
        ulb_id,
        year,

        COUNT(*) AS standardization_record_count,

        BOOL_OR(
            COALESCE(LOWER(BTRIM("isStandardizable"::TEXT)), '') <> 'no'
        ) AS has_standardized_value,

        BOOL_OR(
            LOWER(BTRIM("isStandardizable"::TEXT)) = 'no'
        ) AS has_error_value

    FROM {{ source('afs_digitisation', 'ledgerlogs') }}
    GROUP BY ulb_id, year
),

with_standardization AS (
    SELECT
        aas.ulb_name,
        aas.ulb_code,
        aas.state_name,
        aas.iso_code,
        aas.population_category,
        aas.population_category_sort_order,
        aas.population_category_ordered,
        aas.financial_year,
        aas.audited_status,
        aas.unaudited_status,

        CASE
            -- Ledger/file record exists and isStandardizable is NULL, blank, yes, true, etc.
            -- Anything except explicit 'no' is treated as standardized.
            WHEN COALESCE(l.has_standardized_value, FALSE) = TRUE
                THEN 'standardized'

            -- Ledger/file record exists and isStandardizable is explicitly 'no'.
            WHEN COALESCE(l.has_error_value, FALSE) = TRUE
                THEN 'error'

            -- No ledger/file record exists, but annual account is eligible.
            WHEN l.standardization_record_count IS NULL
                 AND (
                        aas.audited_status = 'eligible'
                     OR aas.unaudited_status = 'eligible'
                 )
                THEN 'yet to standardized'

            ELSE 'Ineligible for standardization'
        END AS is_standadized_by_magc,

        aas.has_annual_account_record

    FROM annual_account_status aas
    LEFT JOIN standardization_logs l 
        ON aas.ulb_id = l.ulb_id 
       AND aas.financial_year = l.year
)

SELECT
    ulb_name,
    ulb_code,
    state_name,
    iso_code,
    financial_year,
    population_category,
    population_category_sort_order,
    population_category_ordered,
    audited_status,
    unaudited_status,
    1 AS total_count,

    CASE
        WHEN is_standadized_by_magc IN ('standardized', 'error')
            THEN 'eligible'

        WHEN audited_status = 'eligible'
          OR unaudited_status = 'eligible'
            THEN 'eligible'

        WHEN audited_status = 'submitted'
          OR unaudited_status = 'submitted'
            THEN 'submitted'

        ELSE 'ineligible'
    END AS status,

    is_standadized_by_magc,
    has_annual_account_record

FROM with_standardization