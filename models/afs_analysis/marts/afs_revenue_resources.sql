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
    SELECT
        state,
        iso_code
    FROM {{ source('cityfinance_prod', 'iso_codes') }}
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

ulb_master AS (
    SELECT DISTINCT
        u.ulb_code,
        u.ulb_name,
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

financial_raw AS (
    SELECT
        BTRIM(l.ulb::TEXT) AS ulb_name,
        BTRIM(l.year::TEXT) AS financial_year,
        COALESCE(NULLIF(l."lineItems"::TEXT, ''), '{}')::JSONB AS lineitems_json
    FROM {{ source('afs_analysis', 'ledgerlogs') }} l
    WHERE
        BTRIM(l.year::TEXT) ~ '^[0-9]{4}-[0-9]{2}$'
        AND CAST(SPLIT_PART(BTRIM(l.year::TEXT), '-', 1) AS INTEGER) BETWEEN 2019 AND 2022
),

tax_line_item_flags AS (
    SELECT
        ulb_name,
        financial_year,
        lineitems_json,

        CASE
            WHEN
                lineitems_json ->> '11001' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11002' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11003' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11004' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11005' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11006' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11007' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11008' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11009' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11010' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11011' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11012' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11013' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11014' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11015' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11016' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11017' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '11018' ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN 1
            ELSE 0
        END AS has_any_tax_line_item_data,

        CASE
            WHEN lineitems_json ->> '120' ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN 1
            ELSE 0
        END AS has_assigned_revenue_data,

        CASE
            WHEN
                lineitems_json ->> '170' ~ '^-?[0-9]+(\.[0-9]+)?$'
                OR lineitems_json ->> '171' ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN 1
            ELSE 0
        END AS has_other_income_data

    FROM financial_raw
),

tax_line_items AS (
    SELECT
        ulb_name,

        NULLIF(
            CONCAT_WS(
                ', ',

                MAX(CASE WHEN lineitems_json ->> '11001' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11001 - Property Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11002' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11002 - Water Supply and Drainage Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11003' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11003 - Sewerage Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11004' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11004 - Conservancy Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11005' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11005 - Lighting Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11006' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11006 - Education Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11007' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11007 - Vehicle Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11008' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11008 - Tax on Animals' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11009' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11009 - Electricity Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11010' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11010 - Professional Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11011' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11011 - Entertainment Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11012' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11012 - Advertisement Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11013' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11013 - Pilgrimage Tax' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11014' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11014 - Octroi & Toll' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11015' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11015 - Cess' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11016' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11016 - Tax on Carts' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11017' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11017 - Tax Remission & Refund' ELSE NULL END),
                MAX(CASE WHEN lineitems_json ->> '11018' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN '11018 - Others' ELSE NULL END)
            ),
            ''
        ) AS tax_line_items_with_data,

        NULLIF(
            MAX(
                CASE
                    WHEN lineitems_json ->> '120' ~ '^-?[0-9]+(\.[0-9]+)?$'
                        THEN 'Tax and Duties collected by others'
                    ELSE NULL
                END
            ),
            ''
        ) AS assigned_revenue_with_data,

        NULLIF(
            CONCAT_WS(
                ', ',

                MAX(
                    CASE
                        WHEN lineitems_json ->> '170' ~ '^-?[0-9]+(\.[0-9]+)?$'
                            THEN '170 - Income from Investment'
                        ELSE NULL
                    END
                ),

                MAX(
                    CASE
                        WHEN lineitems_json ->> '171' ~ '^-?[0-9]+(\.[0-9]+)?$'
                            THEN '171 - Interest earned'
                        ELSE NULL
                    END
                )
            ),
            ''
        ) AS other_income_with_data,

        (
            MAX(CASE WHEN lineitems_json ->> '11001' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11002' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11003' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11004' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11005' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11006' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11007' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11008' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11009' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11010' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11011' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11012' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11013' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11014' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11015' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11016' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11017' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END) +
            MAX(CASE WHEN lineitems_json ->> '11018' ~ '^-?[0-9]+(\.[0-9]+)?$' THEN 1 ELSE 0 END)
        ) AS no_of_tax_line_items_with_data,

        STRING_AGG(
            DISTINCT financial_year,
            ', ' ORDER BY financial_year
        ) FILTER (
            WHERE
                has_any_tax_line_item_data = 1
                OR has_assigned_revenue_data = 1
                OR has_other_income_data = 1
        ) AS years_with_data,

        '2019-20, 2020-21, 2021-22, 2022-23' AS years_checked

    FROM tax_line_item_flags
    GROUP BY
        ulb_name
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

        tli.tax_line_items_with_data,
        tli.assigned_revenue_with_data AS "Assigned Revenue",
        tli.other_income_with_data AS "Other Income",
        tli.no_of_tax_line_items_with_data,
        tli.years_with_data,
        tli.years_checked

    FROM ulb_master um
    INNER JOIN tax_line_items tli
        ON LOWER(BTRIM(um.ulb_name)) = LOWER(BTRIM(tli.ulb_name))
    WHERE
        tli.no_of_tax_line_items_with_data > 0
        OR tli.assigned_revenue_with_data IS NOT NULL
        OR tli.other_income_with_data IS NOT NULL
)

SELECT *
FROM final
ORDER BY
    state_name,
    ulb_name