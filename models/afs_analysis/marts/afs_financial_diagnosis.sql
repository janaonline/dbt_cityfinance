{{ config(materialized = 'table', tags = ['afs_analysis']) }}


WITH states AS (
    SELECT
        _id,
        name
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

ulbtypes AS (
    SELECT
        _id AS type_id,
        name AS ulb_type
    FROM {{ source('afs_analysis', 'ulbtypes') }}
    WHERE "isActive" = 'true'
),

ulbs AS (
    SELECT
        u._id AS ulb_id,
        u.code AS ulb_code,
        u.name AS ulb_name,
        u.population AS population,
        u.area AS area,
        u.state AS state_id,
        ut.ulb_type
    FROM {{ source('cityfinance_prod', 'ulbs') }} u
    INNER JOIN states s
        ON u.state = s._id
    LEFT JOIN ulbtypes ut
        ON u."ulbType" = ut.type_id
    WHERE
        u."isActive" = 'true'
        AND u."isPublish" = 'true'
),

iso_codes AS (
    SELECT
        state,
        iso_code
    FROM {{ source('cityfinance_prod', 'iso_codes') }}
),

years_base AS (
    SELECT
        _id AS year_id,
        year AS financial_year,
        CAST(SPLIT_PART(year, '-', 1) AS INTEGER) AS financial_year_start
    FROM {{ source('cityfinance_prod', 'years') }}
    WHERE year ~ '^\d{4}-\d{2}$'
      AND CAST(SPLIT_PART(year, '-', 1) AS INTEGER) BETWEEN 2018 AND 2023
),

years AS (
    SELECT
        year_id,
        financial_year,
        financial_year_start
    FROM years_base

    UNION ALL

    SELECT
        NULL AS year_id,
        'CAGR' AS financial_year,
        9999 AS financial_year_start
),

total_property_tax_collection AS (
    SELECT
        BTRIM(ptm.ulb::TEXT) AS ulb_id,
        yb.financial_year,
        yb.year_id,
        yb.financial_year_start,
        {{ safe_numeric('ptm.value') }} * 100000 AS value
    FROM {{ source('cityfinance_prod', 'propertytaxopmappers') }} ptm
    LEFT JOIN years_base yb
        ON BTRIM(ptm.year::TEXT) = BTRIM(yb.year_id::TEXT)
    WHERE ptm."displayPriority" = '1.17'
      AND yb.financial_year ~ '^\d{4}-\d{2}$'
),

property_tax_collection_base AS (
    SELECT
        ulb_id,
        financial_year,
        MAX(value) AS total_property_tax_collection
    FROM total_property_tax_collection
    GROUP BY
        ulb_id,
        financial_year
),

property_tax_collection_cagr AS (
    SELECT
        ulb_id,
        'CAGR' AS financial_year,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_property_tax_collection END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_property_tax_collection END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_property_tax_collection END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_property_tax_collection END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_property_tax_collection END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_property_tax_collection END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_property_tax_collection

    FROM property_tax_collection_base
    GROUP BY ulb_id
),

property_tax_collection_all AS (
    SELECT * FROM property_tax_collection_base

    UNION ALL

    SELECT * FROM property_tax_collection_cagr
),

financial_raw AS (
    SELECT
        LOWER(REGEXP_REPLACE(BTRIM(l.ulb::TEXT), '[[:space:]]+', ' ', 'g')) AS ulb_join_key,
        BTRIM(l.ulb::TEXT) AS ulb_name,
        BTRIM(l.year::TEXT) AS financial_year,

        COALESCE(NULLIF(l."lineItems"::TEXT, ''), '{}')::JSONB AS lineitems_json,
        COALESCE(NULLIF(l.indicators::TEXT, ''), '{}')::JSONB AS indicators_json

    FROM {{ source('afs_analysis', 'ledgerlogs') }} l
    WHERE CAST(SPLIT_PART(BTRIM(l.year::TEXT), '-', 1) AS INTEGER) BETWEEN 2019 AND 2023
),

financial_values AS (
    SELECT
        ulb_join_key,
        MAX(ulb_name) AS ulb_name,
        financial_year,

        MAX(
            CASE
                WHEN
                    (lineitems_json ->> '110') ~ '^-?[0-9]+(\.[0-9]+)?$'
                    OR (lineitems_json ->> '130') ~ '^-?[0-9]+(\.[0-9]+)?$'
                    OR (lineitems_json ->> '140') ~ '^-?[0-9]+(\.[0-9]+)?$'
                    OR (lineitems_json ->> '150') ~ '^-?[0-9]+(\.[0-9]+)?$'
                    OR (lineitems_json ->> '180') ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '110') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '110')::NUMERIC
                            ELSE NULL
                        END,
                        0
                    )
                    +
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '130') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '130')::NUMERIC
                            ELSE NULL
                        END,
                        0
                    )
                    +
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '140') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '140')::NUMERIC
                            ELSE NULL
                        END,
                        0
                    )
                    +
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '150') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '150')::NUMERIC
                            ELSE NULL
                        END,
                        0
                    )
                    +
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '180') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '180')::NUMERIC
                            ELSE NULL
                        END,
                        0
                    )
                ELSE NULL
            END
        ) AS total_own_source_revenue,

        MAX(
            CASE
                WHEN lineitems_json ->> '160' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '160')::NUMERIC
                ELSE NULL
            END
        ) AS revenue_grants,

        MAX(
            CASE
                WHEN lineitems_json ->> '120' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '120')::NUMERIC
                ELSE NULL
            END
        ) AS assigned_revenue,

        MAX(
            CASE
                WHEN (lineitems_json ->> '170') ~ '^-?[0-9]+(\.[0-9]+)?$'
                  OR (lineitems_json ->> '171') ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '170') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '170')::NUMERIC
                            ELSE 0
                        END,
                        0
                    )
                    +
                    COALESCE(
                        CASE
                            WHEN (lineitems_json ->> '171') ~ '^-?[0-9]+(\.[0-9]+)?$'
                                THEN (lineitems_json ->> '171')::NUMERIC
                            ELSE 0
                        END,
                        0
                    )
                ELSE NULL
            END
        ) AS other_income,

        MAX(
            CASE
                WHEN indicators_json ->> 'totRevenue' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (indicators_json ->> 'totRevenue')::NUMERIC
                ELSE NULL
            END
        ) AS total_revenue,

        MAX(
            CASE
                WHEN lineitems_json ->> '110' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '110')::NUMERIC
                ELSE NULL
            END
        ) AS tax_revenue,

        MAX(
            CASE
                WHEN lineitems_json ->> '11001' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '11001')::NUMERIC
                ELSE NULL
            END
        ) AS property_tax,

        MAX(
            CASE
                WHEN lineitems_json ->> '330' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '330')::NUMERIC
                ELSE NULL
            END
        ) AS secured_loans,

        MAX(
            CASE
                WHEN lineitems_json ->> '331' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '331')::NUMERIC
                ELSE NULL
            END
        ) AS unsecured_loans,

        MAX(
            CASE
                WHEN lineitems_json ->> '210' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '210')::NUMERIC
                ELSE NULL
            END
        ) AS establishment_expenditure,

        MAX(
            CASE
                WHEN lineitems_json ->> '220' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '220')::NUMERIC
                ELSE NULL
            END
        ) AS administrative_expenses,

        MAX(
            CASE
                WHEN lineitems_json ->> '230' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '230')::NUMERIC
                ELSE NULL
            END
        ) AS operation_and_maintenance,

        MAX(
            CASE
                WHEN lineitems_json ->> '240' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '240')::NUMERIC
                ELSE NULL
            END
        ) AS interest_and_finance_charges,

        MAX(
            CASE
                WHEN lineitems_json ->> '250' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '250')::NUMERIC
                ELSE NULL
            END
        ) AS programme_expenses,

        MAX(
            CASE
                WHEN lineitems_json ->> '260' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '260')::NUMERIC
                ELSE NULL
            END
        ) AS revenue_grants_contributions_and_subsidies,

        MAX(
            CASE
                WHEN lineitems_json ->> '270' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '270')::NUMERIC
                ELSE NULL
            END
        ) AS provisions_and_write_off,

        MAX(
            CASE
                WHEN lineitems_json ->> '271' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '271')::NUMERIC
                ELSE NULL
            END
        ) AS miscellaneous_expenses,

        MAX(
            CASE
                WHEN lineitems_json ->> '272' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '272')::NUMERIC
                ELSE NULL
            END
        ) AS depreciation,

        MAX(
            CASE
                WHEN lineitems_json ->> '280' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '280')::NUMERIC
                ELSE NULL
            END
        ) AS prior_period_items,

        MAX(
            CASE
                WHEN lineitems_json ->> '290' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '290')::NUMERIC
                ELSE NULL
            END
        ) AS transfer_to_reserve_funds,

        MAX(
            CASE
                WHEN lineitems_json ->> '300' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '300')::NUMERIC
                ELSE NULL
            END
        ) AS other,

        MAX(
            CASE
                WHEN indicators_json ->> 'totRevenueExpenditure' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (indicators_json ->> 'totRevenueExpenditure')::NUMERIC
                ELSE NULL
            END
        ) AS total_expenditure

    FROM financial_raw
    GROUP BY
        ulb_join_key,
        financial_year
),

base_metrics AS (
    SELECT
        ulb_join_key,
        ulb_name,
        financial_year,

        total_own_source_revenue,
        revenue_grants,
        assigned_revenue,
        other_income,
        total_revenue,
        tax_revenue,

        total_own_source_revenue - tax_revenue AS non_tax_revenue,

        property_tax,

        ROUND(
            (((total_own_source_revenue - tax_revenue) / NULLIF(total_own_source_revenue, 0)) * 100)::NUMERIC,
            2
        ) AS non_tax_revenue_as_pct_of_osr,

        secured_loans,
        unsecured_loans,
        establishment_expenditure,
        administrative_expenses,
        operation_and_maintenance,
        interest_and_finance_charges,
        programme_expenses,
        revenue_grants_contributions_and_subsidies,
        provisions_and_write_off,
        miscellaneous_expenses,
        depreciation,
        prior_period_items,
        transfer_to_reserve_funds,
        other,
        total_expenditure

    FROM financial_values
),

cagr_metrics AS (
    SELECT
        ulb_join_key,
        MAX(ulb_name) AS ulb_name,
        'CAGR' AS financial_year,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_own_source_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS revenue_grants,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN assigned_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN assigned_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN assigned_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN assigned_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN assigned_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN assigned_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS assigned_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other_income END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other_income END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other_income END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other_income END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN other_income END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN other_income END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS other_income,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS tax_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS non_tax_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN property_tax END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN property_tax END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS property_tax,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue_as_pct_of_osr END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue_as_pct_of_osr END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue_as_pct_of_osr END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue_as_pct_of_osr END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS non_tax_revenue_as_pct_of_osr,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN secured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN secured_loans END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN secured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN secured_loans END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN secured_loans END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN secured_loans END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS secured_loans,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN unsecured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN unsecured_loans END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN unsecured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN unsecured_loans END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN unsecured_loans END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN unsecured_loans END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS unsecured_loans,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN establishment_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN establishment_expenditure END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN establishment_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN establishment_expenditure END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN establishment_expenditure END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN establishment_expenditure END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS establishment_expenditure,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN administrative_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN administrative_expenses END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN administrative_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN administrative_expenses END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN administrative_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN administrative_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS administrative_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN operation_and_maintenance END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN operation_and_maintenance END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN operation_and_maintenance END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN operation_and_maintenance END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN operation_and_maintenance END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN operation_and_maintenance END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS operation_and_maintenance,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN interest_and_finance_charges END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN interest_and_finance_charges END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN interest_and_finance_charges END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN interest_and_finance_charges END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN interest_and_finance_charges END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN interest_and_finance_charges END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS interest_and_finance_charges,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN programme_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN programme_expenses END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN programme_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN programme_expenses END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN programme_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN programme_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS programme_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants_contributions_and_subsidies END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants_contributions_and_subsidies END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants_contributions_and_subsidies END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants_contributions_and_subsidies END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants_contributions_and_subsidies END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants_contributions_and_subsidies END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS revenue_grants_contributions_and_subsidies,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN provisions_and_write_off END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN provisions_and_write_off END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN provisions_and_write_off END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN provisions_and_write_off END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN provisions_and_write_off END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN provisions_and_write_off END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS provisions_and_write_off,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN miscellaneous_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN miscellaneous_expenses END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN miscellaneous_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN miscellaneous_expenses END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN miscellaneous_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN miscellaneous_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS miscellaneous_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN depreciation END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN depreciation END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN depreciation END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN depreciation END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN depreciation END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN depreciation END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS depreciation,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN prior_period_items END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN prior_period_items END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN prior_period_items END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN prior_period_items END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN prior_period_items END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN prior_period_items END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS prior_period_items,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN transfer_to_reserve_funds END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN transfer_to_reserve_funds END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN transfer_to_reserve_funds END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN transfer_to_reserve_funds END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN transfer_to_reserve_funds END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN transfer_to_reserve_funds END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS transfer_to_reserve_funds,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN other END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN other END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS other,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_expenditure END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_expenditure END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_expenditure END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_expenditure END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_expenditure

    FROM base_metrics
    GROUP BY ulb_join_key
),

financials_all AS (
    SELECT * FROM base_metrics

    UNION ALL

    SELECT * FROM cagr_metrics
),

bonds AS (
    SELECT
        UPPER(BTRIM("ULB_Code"::TEXT)) AS ulb_code,

        MAX("No__of_Bonds_Raised"::NUMERIC) AS no_of_bonds_raised,

        MAX("Amount_Raised__In_Cr__"::NUMERIC) AS amount_raised_in_cr

    FROM {{ source('afs_analysis', 'bonds_afs_finance_analysis') }}
    GROUP BY
        UPPER(BTRIM("ULB_Code"::TEXT))
),

final_base AS (
    SELECT
        u.ulb_id,
        u.ulb_code,
        u.ulb_name,
        s.name AS state_name,
        i.iso_code,
        y.year_id,
        y.financial_year,
        u.ulb_type,
        u.population,
        u.area
    FROM ulbs u
    CROSS JOIN years y
    LEFT JOIN states s
        ON u.state_id = s._id
    LEFT JOIN iso_codes i
        ON s.name = i.state
),


financials_total_adjusted_base AS (
    SELECT
        fb.ulb_id,
        fb.ulb_name,
        fb.financial_year,

        CASE
            WHEN bm.total_own_source_revenue IS NULL
             AND bm.property_tax IS NULL
             AND ptc.total_property_tax_collection IS NULL
                THEN NULL
            ELSE
                COALESCE(bm.total_own_source_revenue, 0)
                - COALESCE(bm.property_tax, 0)
                + COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0)
        END AS total_own_source_revenue,

        CASE
            WHEN bm.total_revenue IS NULL
             AND bm.property_tax IS NULL
             AND ptc.total_property_tax_collection IS NULL
                THEN NULL
            ELSE
                COALESCE(bm.total_revenue, 0)
                - COALESCE(bm.property_tax, 0)
                + COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0)
        END AS total_revenue,

        ROUND(
            (COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0) / NULLIF(
                CASE
                    WHEN bm.total_own_source_revenue IS NULL
                     AND bm.property_tax IS NULL
                     AND ptc.total_property_tax_collection IS NULL
                        THEN NULL
                    ELSE
                        COALESCE(bm.total_own_source_revenue, 0)
                        - COALESCE(bm.property_tax, 0)
                        + COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0)
                END, 0
            ) * 100)::NUMERIC,
            2
        ) AS property_tax_as_pct_of_osr,

        ROUND(
            (
                (COALESCE(bm.tax_revenue, 0) - COALESCE(bm.property_tax, 0) + COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0))
                / NULLIF(
                    CASE
                        WHEN bm.total_own_source_revenue IS NULL
                         AND bm.property_tax IS NULL
                         AND ptc.total_property_tax_collection IS NULL
                            THEN NULL
                        ELSE
                            COALESCE(bm.total_own_source_revenue, 0)
                            - COALESCE(bm.property_tax, 0)
                            + COALESCE(ptc.total_property_tax_collection, bm.property_tax, 0)
                    END, 0
                ) * 100
            )::NUMERIC,
            2
        ) AS tax_revenue_as_pct_of_osr

    FROM final_base fb
    LEFT JOIN base_metrics bm
        ON LOWER(REGEXP_REPLACE(BTRIM(fb.ulb_name::TEXT), '[[:space:]]+', ' ', 'g')) = bm.ulb_join_key
        AND fb.financial_year = bm.financial_year
    LEFT JOIN property_tax_collection_base ptc
        ON BTRIM(fb.ulb_id::TEXT) = BTRIM(ptc.ulb_id::TEXT)
        AND fb.financial_year = ptc.financial_year
    WHERE fb.financial_year <> 'CAGR'
),

financials_total_adjusted_cagr AS (
    SELECT
        ulb_id,
        ulb_name,
        'CAGR' AS financial_year,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_own_source_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax_as_pct_of_osr END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax_as_pct_of_osr END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN property_tax_as_pct_of_osr END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN property_tax_as_pct_of_osr END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS property_tax_as_pct_of_osr,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue_as_pct_of_osr END) IS NULL
                THEN -100
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue_as_pct_of_osr END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue_as_pct_of_osr END) >= 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue_as_pct_of_osr END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue_as_pct_of_osr END), 0),
                    1.0 / 3 
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS tax_revenue_as_pct_of_osr

    FROM financials_total_adjusted_base
    GROUP BY
        ulb_id,
        ulb_name
),

financials_total_adjusted_all AS (
    SELECT * FROM financials_total_adjusted_base

    UNION ALL

    SELECT * FROM financials_total_adjusted_cagr
)

SELECT
    fb.ulb_code,
    fb.ulb_name,
    fb.state_name,
    fb.iso_code,
    fb.financial_year,
    fb.ulb_type,
    fb.population,
    fb.area,
    b.no_of_bonds_raised AS "No Of Bonds Raised",
    b.amount_raised_in_cr AS "Amount Raised in Cr",
    
    -- NEW: Pulling the Initial (unadjusted) Total Own Source Revenue from fa
    fa.total_own_source_revenue AS "Initial Total Own Source Revenue",
    
    -- Original: The newly adjusted Total Own Source Revenue from fta
    fta.total_own_source_revenue AS "Total Own Source Revenue",
    
    fa.revenue_grants AS "Revenue Grants",
    fa.assigned_revenue AS "Assigned Revenue",
    fa.other_income AS "Other Income",

    fa.total_revenue AS "Intial Total Revenue",

    fta.total_revenue AS "Total Revenue",
    fa.tax_revenue AS "Tax Revenue",
    fa.non_tax_revenue AS "Non-Tax Revenue",
    fa.property_tax AS "Property Tax",

    ptc.total_property_tax_collection AS "Total Property Tax Collection",

    fta.tax_revenue_as_pct_of_osr AS "Tax Revenue as % of OSR",
    fa.non_tax_revenue_as_pct_of_osr AS "Non-Tax Revenue as % of OSR",
    fta.property_tax_as_pct_of_osr AS "Property Tax as % of OSR",

    fa.secured_loans AS "Secured Loans",
    fa.unsecured_loans AS "Unsecured Loans",

    fa.establishment_expenditure AS "Establishment Expenditure",
    fa.administrative_expenses AS "Administrative Expenses",
    fa.operation_and_maintenance AS "Operation and Maintenance",
    fa.interest_and_finance_charges AS "Interest and Finance Charges",
    fa.programme_expenses AS "Programme Expenses",
    fa.revenue_grants_contributions_and_subsidies AS "Revenue Grants, Contributions, and Subsidies",
    fa.provisions_and_write_off AS "Provisions and Write Off",
    fa.miscellaneous_expenses AS "Miscellaneous Expenses",
    fa.depreciation AS "Depreciation",
    fa.prior_period_items AS "Prior Period Items",
    fa.transfer_to_reserve_funds AS "Transfer to Reserve Funds",
    fa.other AS "Other",
    fa.total_expenditure AS "Total Expenditure"

FROM final_base fb
LEFT JOIN financials_all fa
    ON LOWER(REGEXP_REPLACE(BTRIM(fb.ulb_name::TEXT), '[[:space:]]+', ' ', 'g')) = fa.ulb_join_key
    AND fb.financial_year = fa.financial_year

LEFT JOIN financials_total_adjusted_all fta
    ON BTRIM(fb.ulb_id::TEXT) = BTRIM(fta.ulb_id::TEXT)
    AND fb.financial_year = fta.financial_year

LEFT JOIN property_tax_collection_all ptc
    ON BTRIM(fb.ulb_id::TEXT) = BTRIM(ptc.ulb_id::TEXT)
    AND fb.financial_year = ptc.financial_year

LEFT JOIN bonds b
    ON UPPER(BTRIM(fb.ulb_code::TEXT)) = b.ulb_code

ORDER BY
    fb.state_name,
    fb.ulb_name,
    CASE
        WHEN fb.financial_year = 'CAGR' THEN 9999
        ELSE CAST(SPLIT_PART(fb.financial_year, '-', 1) AS INTEGER)
    END