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

iso_codes AS (
    SELECT DISTINCT ON (state)
        state,
        iso_code
    FROM {{ source('cityfinance_prod', 'iso_codes') }}
    ORDER BY
        state,
        iso_code
),

-- Maps the CityFinance year ID to its financial-year label.
years_base AS (
    SELECT
        _id AS year_id,
        year AS financial_year,
        CAST(
            SPLIT_PART(year, '-', 1) AS INTEGER
        ) AS financial_year_start
    FROM {{ source('cityfinance_prod', 'years') }}
    WHERE
        year ~ '^[0-9]{4}-[0-9]{2}$'
        AND CAST(
            SPLIT_PART(year, '-', 1) AS INTEGER
        ) BETWEEN 2019 AND 2022
),

ulbs AS (
    SELECT
        u._id AS ulb_id,
        u.code AS ulb_code,
        u.name AS ulb_name,
        u.state AS state_id,
        u.population,
        u.area,
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

ulb_master_raw AS (
    SELECT
        BTRIM(u.ulb_id::TEXT) AS ulb_id,
        u.ulb_code,
        u.ulb_name,
        LOWER(BTRIM(u.ulb_name)) AS ulb_name_key,
        s.name AS state_name,
        i.iso_code,
        u.ulb_type,
        u.population,
        u.area
    FROM ulbs u
    LEFT JOIN states s
        ON u.state_id = s._id
    LEFT JOIN iso_codes i
        ON s.name = i.state
),

-- Ensures only one master row per ULB code.
ulb_master AS (
    SELECT DISTINCT ON (ulb_code)
        ulb_id,
        ulb_code,
        ulb_name,
        ulb_name_key,
        state_name,
        iso_code,
        ulb_type,
        population,
        area
    FROM ulb_master_raw
    ORDER BY
        ulb_code,
        state_name,
        ulb_name
),

financial_raw AS (
    SELECT
        LOWER(BTRIM(l.ulb::TEXT)) AS ulb_name_key,
        BTRIM(l.ulb::TEXT) AS ulb_name,
        BTRIM(l.year::TEXT) AS financial_year,
        COALESCE(
            NULLIF(l."lineItems"::TEXT, ''),
            '{}'
        )::JSONB AS lineitems_json
    FROM {{ source('afs_analysis', 'ledgerlogs') }} l
    WHERE
        BTRIM(l.year::TEXT) ~ '^[0-9]{4}-[0-9]{2}$'
        AND CAST(
            SPLIT_PART(BTRIM(l.year::TEXT), '-', 1) AS INTEGER
        ) BETWEEN 2019 AND 2022
),

/*
Property Tax collection from Property Tax OPM.

displayPriority 1.17 represents Total Property Tax Collection.
The stored value is in lakhs and is converted to INR.
*/
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
    WHERE
        ptm."displayPriority" = '1.17'
        AND yb.financial_year ~ '^[0-9]{4}-[0-9]{2}$'
),

line_item_master AS (
    SELECT *
    FROM (
        VALUES
            -- Tax Revenue
            ('11001', 1, 'tax', 'Property Tax*'),
            ('11002', 2, 'tax', 'Water Supply and Drainage Tax'),
            ('11003', 3, 'tax', 'Sewerage Tax'),
            ('11004', 4, 'tax', 'Conservancy Tax'),
            ('11005', 5, 'tax', 'Lighting Tax'),
            ('11006', 6, 'tax', 'Education Tax'),
            ('11007', 7, 'tax', 'Vehicle Tax'),
            ('11008', 8, 'tax', 'Tax on Animals'),
            ('11009', 9, 'tax', 'Electricity Tax'),
            ('11010', 10, 'tax', 'Professional Tax'),
            ('11011', 11, 'tax', 'Entertainment Tax'),
            ('11012', 12, 'tax', 'Advertisement Tax'),
            ('11013', 13, 'tax', 'Pilgrimage Tax'),
            ('11014', 14, 'tax', 'Octroi & Toll'),
            ('11015', 15, 'tax', 'Cess'),
            ('11016', 16, 'tax', 'Tax on Carts'),
            ('11017', 17, 'tax', 'Tax Remission & Refund'),
            ('11018', 18, 'tax', 'Others'),

            -- Assigned Revenue
            (
                '120',
                101,
                'assigned_revenue',
                'Tax and Duties collected by others'
            ),

            -- Non-Tax Revenue
            (
                '130',
                201,
                'non_tax_revenue',
                'Rental Income from Municipal Properties'
            ),
            (
                '140',
                202,
                'non_tax_revenue',
                'Fee & User Charges'
            ),
            (
                '150',
                203,
                'non_tax_revenue',
                'Sale & Hire charges'
            ),
            (
                '180',
                204,
                'non_tax_revenue',
                'Other Non-Tax Revenue (Insurance Claim Recovery, Miscellaneous)'
            ),

            -- Other Income
            (
                '170',
                301,
                'other_income',
                'Income from Investment'
            ),
            (
                '171',
                302,
                'other_income',
                'Interest earned'
            )
    ) AS m(
        line_item_code,
        sort_order,
        category,
        label
    )
),

/*
All ledger-based line-item hits except Property Tax.

Property Tax line item 11001 is explicitly excluded because it is now
sourced from propertytaxopmappers.
*/
ledger_line_item_hits AS (
    SELECT
        um.ulb_id,
        fr.financial_year,
        m.category,
        m.line_item_code,
        m.sort_order,
        m.label
    FROM financial_raw fr
    INNER JOIN ulb_master um
        ON fr.ulb_name_key = um.ulb_name_key
    INNER JOIN line_item_master m
        ON fr.lineitems_json ->> m.line_item_code
            ~ '^-?[0-9]+(\.[0-9]+)?$'
    WHERE
        m.line_item_code <> '11001'
),

/*
Creates a Property Tax line-item hit when a valid Property Tax
collection value is available for the ULB and financial year.

A zero value is treated as available data, consistent with the
existing ledger-line-item logic.
*/
property_tax_line_item_hits AS (
    SELECT DISTINCT
        um.ulb_id,
        ptc.financial_year,
        'tax' AS category,
        '11001' AS line_item_code,
        1 AS sort_order,
        'Property Tax*' AS label
    FROM total_property_tax_collection ptc
    INNER JOIN ulb_master um
        ON ptc.ulb_id = um.ulb_id
    WHERE
        ptc.value IS NOT NULL
),

-- Combines ledger-based line items with Property Tax OPM data.
line_item_hits AS (
    SELECT
        ulb_id,
        financial_year,
        category,
        line_item_code,
        sort_order,
        label
    FROM ledger_line_item_hits

    UNION ALL

    SELECT
        ulb_id,
        financial_year,
        category,
        line_item_code,
        sort_order,
        label
    FROM property_tax_line_item_hits
),

-- Deduplicates the same line item across years.
deduped_line_item_hits AS (
    SELECT
        ulb_id,
        category,
        line_item_code,
        MIN(sort_order) AS sort_order,
        MAX(label) AS label
    FROM line_item_hits
    GROUP BY
        ulb_id,
        category,
        line_item_code
),

-- Compiles all years into one row per ULB.
compiled_line_items AS (
    SELECT
        ulb_id,

        STRING_AGG(
            label,
            ', ' ORDER BY sort_order
        ) FILTER (
            WHERE category = 'tax'
        ) AS tax_line_items_with_data,

        STRING_AGG(
            label,
            ', ' ORDER BY sort_order
        ) FILTER (
            WHERE category = 'assigned_revenue'
        ) AS assigned_revenue_with_data,

        STRING_AGG(
            label,
            ', ' ORDER BY sort_order
        ) FILTER (
            WHERE category = 'other_income'
        ) AS other_income_with_data,

        STRING_AGG(
            label,
            ', ' ORDER BY sort_order
        ) FILTER (
            WHERE category = 'non_tax_revenue'
        ) AS non_tax_revenue_with_data,

        COUNT(*) FILTER (
            WHERE category = 'tax'
        ) AS no_of_tax_line_items_with_data

    FROM deduped_line_item_hits
    GROUP BY
        ulb_id
),

compiled_years AS (
    SELECT
        ulb_id,
        STRING_AGG(
            DISTINCT financial_year,
            ', ' ORDER BY financial_year
        ) AS years_with_data
    FROM line_item_hits
    GROUP BY
        ulb_id
),

final AS (
    SELECT
        um.ulb_code,
        um.ulb_name,
        um.state_name,
        um.iso_code,
        um.ulb_type,
        um.population,
        um.area,

        cli.tax_line_items_with_data AS "Tax Revenue",
        cli.assigned_revenue_with_data AS "Assigned Revenue",
        cli.other_income_with_data AS "Other Income",
        cli.non_tax_revenue_with_data AS "Non-Tax Revenue",
        cli.no_of_tax_line_items_with_data,
        cy.years_with_data,
        '2019-20, 2020-21, 2021-22, 2022-23' AS years_checked

    FROM ulb_master um
    LEFT JOIN compiled_line_items cli
        ON um.ulb_id = cli.ulb_id
    LEFT JOIN compiled_years cy
        ON um.ulb_id = cy.ulb_id
)

SELECT *
FROM final
ORDER BY
    state_name,
    ulb_name