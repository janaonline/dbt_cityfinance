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

years AS (
    SELECT
        year AS financial_year,
        CAST(SPLIT_PART(year, '-', 1) AS INTEGER) AS financial_year_start
    FROM {{ source('cityfinance_prod', 'years') }}
    WHERE CAST(SPLIT_PART(year, '-', 1) AS INTEGER) BETWEEN 2019 AND 2022

    UNION ALL

    SELECT
        'CAGR' AS financial_year,
        9999 AS financial_year_start
),

financial_raw AS (
    SELECT
        BTRIM(l.ulb::TEXT) AS ulb_name,
        BTRIM(l.year::TEXT) AS financial_year,

        COALESCE(NULLIF(l."lineItems"::TEXT, ''), '{}')::JSONB AS lineitems_json,
        COALESCE(NULLIF(l.indicators::TEXT, ''), '{}')::JSONB AS indicators_json

    FROM {{ source('afs_analysis', 'ledgerlogs') }} l
    WHERE CAST(SPLIT_PART(BTRIM(l.year::TEXT), '-', 1) AS INTEGER) BETWEEN 2019 AND 2022
),

financial_values AS (
    SELECT
        ulb_name,
        financial_year,

        MAX(
            CASE
                WHEN indicators_json ->> 'totOwnRevenue' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (indicators_json ->> 'totOwnRevenue')::NUMERIC
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
                WHEN lineitems_json ->> '180' ~ '^-?[0-9]+(\.[0-9]+)?$'
                    THEN (lineitems_json ->> '180')::NUMERIC
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
        ulb_name,
        financial_year
),

base_metrics AS (
    SELECT
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
            ((tax_revenue / NULLIF(total_own_source_revenue, 0)) * 100)::NUMERIC,
            2
        ) AS tax_revenue_as_pct_of_osr,

        ROUND(
            (((total_own_source_revenue - tax_revenue) / NULLIF(total_own_source_revenue, 0)) * 100)::NUMERIC,
            2
        ) AS non_tax_revenue_as_pct_of_osr,

        ROUND(
            ((property_tax / NULLIF(total_own_source_revenue, 0)) * 100)::NUMERIC,
            2
        ) AS property_tax_as_pct_of_osr,

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
        ulb_name,
        'CAGR' AS financial_year,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_own_source_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_own_source_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_own_source_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS revenue_grants,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN assigned_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN assigned_revenue END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN assigned_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN assigned_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS assigned_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other_income END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other_income END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN other_income END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN other_income END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS other_income,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS tax_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS non_tax_revenue,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN property_tax END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN property_tax END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS property_tax,

        CASE
    WHEN MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue_as_pct_of_osr END) > 0
     AND MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue_as_pct_of_osr END) > 0
        THEN ROUND(((POWER(
            MAX(CASE WHEN financial_year = '2022-23' THEN tax_revenue_as_pct_of_osr END)
            / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN tax_revenue_as_pct_of_osr END), 0),
            1.0 / 3
        ) - 1) * 100)::NUMERIC, 2)
    ELSE NULL
END AS tax_revenue_as_pct_of_osr,

CASE
    WHEN MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue_as_pct_of_osr END) > 0
     AND MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue_as_pct_of_osr END) > 0
        THEN ROUND(((POWER(
            MAX(CASE WHEN financial_year = '2022-23' THEN non_tax_revenue_as_pct_of_osr END)
            / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN non_tax_revenue_as_pct_of_osr END), 0),
            1.0 / 3
        ) - 1) * 100)::NUMERIC, 2)
    ELSE NULL
END AS non_tax_revenue_as_pct_of_osr,

CASE
    WHEN MAX(CASE WHEN financial_year = '2019-20' THEN property_tax_as_pct_of_osr END) > 0
     AND MAX(CASE WHEN financial_year = '2022-23' THEN property_tax_as_pct_of_osr END) > 0
        THEN ROUND(((POWER(
            MAX(CASE WHEN financial_year = '2022-23' THEN property_tax_as_pct_of_osr END)
            / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN property_tax_as_pct_of_osr END), 0),
            1.0 / 3
        ) - 1) * 100)::NUMERIC, 2)
    ELSE NULL
END AS property_tax_as_pct_of_osr,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN secured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN secured_loans END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN secured_loans END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN secured_loans END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS secured_loans,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN unsecured_loans END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN unsecured_loans END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN unsecured_loans END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN unsecured_loans END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS unsecured_loans,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN establishment_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN establishment_expenditure END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN establishment_expenditure END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN establishment_expenditure END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS establishment_expenditure,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN administrative_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN administrative_expenses END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN administrative_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN administrative_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS administrative_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN operation_and_maintenance END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN operation_and_maintenance END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN operation_and_maintenance END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN operation_and_maintenance END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS operation_and_maintenance,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN interest_and_finance_charges END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN interest_and_finance_charges END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN interest_and_finance_charges END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN interest_and_finance_charges END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS interest_and_finance_charges,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN programme_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN programme_expenses END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN programme_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN programme_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS programme_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants_contributions_and_subsidies END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants_contributions_and_subsidies END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN revenue_grants_contributions_and_subsidies END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN revenue_grants_contributions_and_subsidies END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS revenue_grants_contributions_and_subsidies,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN provisions_and_write_off END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN provisions_and_write_off END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN provisions_and_write_off END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN provisions_and_write_off END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS provisions_and_write_off,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN miscellaneous_expenses END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN miscellaneous_expenses END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN miscellaneous_expenses END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN miscellaneous_expenses END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS miscellaneous_expenses,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN depreciation END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN depreciation END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN depreciation END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN depreciation END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS depreciation,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN prior_period_items END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN prior_period_items END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN prior_period_items END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN prior_period_items END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS prior_period_items,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN transfer_to_reserve_funds END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN transfer_to_reserve_funds END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN transfer_to_reserve_funds END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN transfer_to_reserve_funds END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS transfer_to_reserve_funds,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN other END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN other END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN other END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN other END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS other,

        CASE
            WHEN MAX(CASE WHEN financial_year = '2019-20' THEN total_expenditure END) > 0
             AND MAX(CASE WHEN financial_year = '2022-23' THEN total_expenditure END) > 0
                THEN ROUND(((POWER(
                    MAX(CASE WHEN financial_year = '2022-23' THEN total_expenditure END)
                    / NULLIF(MAX(CASE WHEN financial_year = '2019-20' THEN total_expenditure END), 0),
                    1.0 / 3
                ) - 1) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS total_expenditure

    FROM base_metrics
    GROUP BY ulb_name
),

financials_all AS (
    SELECT * FROM base_metrics

    UNION ALL

    SELECT * FROM cagr_metrics
),

final_base AS (
    SELECT
        u.ulb_code,
        u.ulb_name,
        s.name AS state_name,
        i.iso_code,
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

    fa.total_own_source_revenue AS "Total Own Source Revenue",
    fa.revenue_grants AS "Revenue Grants",
    fa.assigned_revenue AS "Assigned Revenue",
    fa.other_income AS "Other Income",
    fa.total_revenue AS "Total Revenue",
    fa.tax_revenue AS "Tax Revenue",
    fa.non_tax_revenue AS "Non-Tax Revenue",
    fa.property_tax AS "Property Tax",

    fa.tax_revenue_as_pct_of_osr AS "Tax Revenue as % of OSR",
    fa.non_tax_revenue_as_pct_of_osr AS "Non-Tax Revenue as % of OSR",
    fa.property_tax_as_pct_of_osr AS "Property Tax as % of OSR",

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
    ON LOWER(BTRIM(fb.ulb_name)) = LOWER(BTRIM(fa.ulb_name))
    AND fb.financial_year = fa.financial_year

ORDER BY
    fb.state_name,
    fb.ulb_name,
    CASE
        WHEN fb.financial_year = 'CAGR' THEN 9999
        ELSE CAST(SPLIT_PART(fb.financial_year, '-', 1) AS INTEGER)
    END