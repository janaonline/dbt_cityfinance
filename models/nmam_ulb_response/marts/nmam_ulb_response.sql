{{ config(
    materialized = 'table',
    alias = 'nmam_ulb_response',
    tags = ['nmam_ulb_response']
) }}

{% set nmam_relation = source('afs_analysis', 'nmam_ulb_response') %}

{#
  Each item contains:
  1. Expected Google Form header
  2. Stable output column alias
#}
{% set requested_columns = [
    ["Timestamp", "response_timestamp"],
    ["Email address", "respondent_email_address"],
    ["ULB Tier", "ulb_tier"],
    ["Name of the Responding Official", "responding_official_name"],
    ["Designation of the Responding Official", "responding_official_designation"],
    ["Email ID of the Official", "official_email_id"],
    ["Mobile Number (WhatsApp-enabled) of the Official", "official_mobile_number"],
    ["Name of Municipal Commissioner / Executive Officer (as applicable)", "municipal_commissioner_or_executive_officer"],
    ["Name of Chartered Accountant Firm Engaged by the ULB (if applicable)", "ca_firm_name"],
    ["CA Membership Number (if applicable)", "ca_membership_number"],
    ["Dominant Digital Platform/Application in use (Please specify the platform name, e.g., K-SMART, SUJOG, UPYOG, State-developed Platform etc. )", "dominant_digital_platform"],
    ["Year of platform's first deployment", "platform_first_deployment_year"],
    ["Number of ULBs on the platform (Applicable only for state-level/centralized responses; please indicate the total number of ULBs covered by the platform)", "number_of_ulbs_on_platform"],
    ["What software or ERP supports digital application/platform? (e.g., Oracle, SAP, ABM, Custom ERP, etc)", "software_or_erp"],
    ["Implementation Partner (e.g., in-house IT team, software vendor, system implementation agency)", "implementation_partner"],
    ["Q1: Is the budget prepared using the NMAM Chart of Accounts structure (Fund, Function, Functionary/Department, Major, Minor, and Detailed Heads) and aligned with accrual accounting principles?", "q01"],
    ["Q2: How is the annual ULB budget prepared and approved? Is it manually on registers or digital system is adopted to process budget compilation and referencing in F&A", "q02"],
    ["Q3: How are expenditure budget & capital works budgets linked to expense accounting & project accounting and payments?", "q03"],
    ["Q4: Are grants (15th FC, state scheme, etc) tracked head-by-head against utilisation?", "q04"],
    ["Q5: When property tax and user charge (water, sewerage, solid waste) demand is raised, is it recorded in the books of accounts on an accrual basis — i.e., is income recognised at the point of billing, with collections accounted as realisation against that demand?", "q05"],
    ["Q6: How is demand for property tax and user charges (water, sewerage, solid waste) generated, billed, and accounted on F&A system? Is system implemented for all revenue sources? Is all revenue source systems integrated with accounting", "q06"],
    ["Q7: What proportion of revenue is collected through digital channels?", "q07"],
    ["Q8: How is Third party Collection integration & Payment Gateway integration processed for collection and reconciliation ? Please answer this question considering the most frequently used third-party collection mechanism ; (e.g., RTGS, NEFT, payment gateways, POS systems, or third-party collection aggregators)", "q08"],
    ["Q9: Are reversals, refunds, and adjustments (e.g., cheque bounce reversals, EMD refunds, demand adjustments) captured with full audit trail?", "q09"],
    ["Q10: When a liability for expenditure arises (e.g., goods/services received, salary earned, contractor work completed), is it recorded in the books of accounts at that point — as an accrual — or only when payment is actually made?", "q10"],
    ["Q11: How is the expenditure cycle — from purchase/work order to bill verification, accrual entry, and payment — processed and recorded in the system?", "q11"],
    ["Q12: How are statutory deductions (TDS, GST, EPF) handled?", "q12"],
    ["Q13: How is contractor payment progress linked to works execution?", "q13"],
    ["Q14: How are fixed assets recognized, valued, and maintained in the books of accounts throughout their lifecycle?", "q14"],
    ["Q15: How are fixed assets recognized, accounted for, and managed throughout their lifecycle—from acquisition to disposal—within the Finance & Accounting system? Is Asset module adopted?", "q15"],
    ["Q16: How comprehensively are accrual accounting principles adopted and reflected in the ULB's accounting practices?", "q16"],
    ["Q17: Is the accounting aligned to NMAM Chart of Accounts structure (Fund, Function, Functionary/Department, Major, Minor, and Detailed Heads)", "q17"],
    ["Q18: To what extent is the Finance & Accounting (F&A) system integrated across the financial lifecycle and capable of supporting accrual accounting?", "q18"],
    ["Q19: Are provisions, accruals at year-end, and prior-period adjustments handled in system?", "q19"],
    ["Q20: Is inter-fund and inter-departmental accounting clearly defined, systematically managed, and supported within the accounting system?", "q20"],
    ["Q21: How is the opening balance for the financial year established & verified?", "q21"],
    ["Q22: What proportion of F&A transactions are originated in a digital system?", "q22"],
    ["Q23: What proportion of ULB departments are on the same accounting platform?", "q23"],
    ["Q24: Is the platform on a managed, supported stack with active vendor or in-house team?", "q24"],
    ["Q25: Is master data (CoA structure, properties, vendors, employees) governed centrally?", "q25"],
    ["Q26: Does the system use standardized data formats and structures that enable seamless information exchange with other government systems and support consistent financial reporting", "q26"],
    ["Q27: Are APIs available for downstream consumers (state UDD, CAG, citizens) and dashboarding?", "q27"],
    ["Q28: Is there a documented data dictionary and schema for fiscal data?", "q28"],
    ["Q29: Are ULB finance staff trained on the current digital MFM system?", "q29"],
    ["Q30: How is ownership, management, and support for the Finance & Accounting (F&A) system organized within the State/ULB", "q30"],
    ["Q31: Is information security (data, access, infra) governed by a formal policy?", "q31"],
    ["Q32: How are budget and resourcing for digital MFM secured year-on-year?", "q32"],
    ["Q33: Are Annual Financial Statements generated directly from system-recorded transactions?", "q33"],
    ["Q34: What is the typical lag between financial year-end and AFS publication?", "q34"],
    ["Q35: Are AFS (Annual Financial Statements) submitted to the State PAG / DLFA in NMAM-prescribed format?", "q35"],
    ["Q36: Are key fiscal indicators published publicly (e.g. revenue receipts, expenditure heads, grant utilisation)?", "q36"],
    ["Q37: Are all financial transactions logged with user, timestamp, and action detail?", "q37"],
    ["Q38: Can the CAG / PAG / DLFA directly access system records during audit?", "q38"],
    ["Q39: Are audit observations tracked and resolved within system?", "q39"],
    ["Q40: Are reconciliations (bank, inter-fund, PFMS, vendor) automated and visible?", "q40"],
    ["Please upload supporting documentation, if any", "supporting_documentation"]
] %}

{% set question_options = {
    "q01": [
        "No alignment with NMAM. Custom budget code/COA with Cash basis budget. Budget structure is different from accounting structure.",
        "No alignment with NMAM. Accrual budgeting adopted. Budget structure is different from accounting structure.",
        "NMAM structure adopted for budgeting on accrual basis. Budget defined based on NMAM CoA with Function or Fund used. Accounting alignment does not exist.",
        "Full NMAM-aligned CoA adopted for budgeting, accounting & reporting. (CoA, Fund, Function, Department)",
        "Full NMAM-aligned as above plus mapped to state and central CoA for interoperability",
    ],
    "q02": [
        "No standard format; compiled and approved entirely on paper.",
        "Templated spreadsheet used for preparation; printed and approved manually.",
        "Spreadsheet-based approved budget uploaded into F&A module to enable budget checks.",
        "Budget Module exist for end-to-end budget lifecycle management - preparation & approval including appropriations. Integrated with F&A module",
        "Budget module exist for preparation and approval with system driven insights. Linkages to IFMS/PFMS CoA",
    ],
    "q03": [
        "No linkage between budget, project execution and payments",
        "Manual linkage using Budget reference numbers while accounting",
        "Budget Linked within F&A but not with source Expenditure modules - Works, Stores, etc(Alerts on Budget overruns)",
        "Budget Linked within F&A and Integrated with Expenditure modules (works / project management). Auto block on budget overruns",
        "Full integration between, Budget, works, payments, and system driven outcome tracking",
    ],
    "q04": [
        "Not tracked at head level",
        "Tracked manually in registers",
        "Tracked digitally in F&A with periodic reconciliation",
        "Real-time digital tracking with auto-utilisation certificates",
        "Integrated with PFMS / state IFMIS for live utilisation flow",
    ],
    "q05": [
        "Cash basis - Income is recorded when cash/collection is received. No demand accounting entry is made in the books; arrears and receivables are not reflected in accounts.",
        "Demand/billing is recorded in a separate register or system, but accounts continue to recognise revenue only on collection.",
        "Revenue is recognised on cash basis through the year, with a manual year-end adjustment entry to record accrued income/receivables based on outstanding demand — typically done for AFS preparation only",
        "Demand raised in the billing system is used to compute accrual entries (PT and/or User charges), but these are manually posted into the accounting/F&A module rather than auto-generated.",
        "Demand raised automatically (for PT and others). Generates an accrual entry in the accounting module at the point of billing & changes; collections are recorded against the receivable in real-time.",
    ],
    "q06": [
        "No system used; demand, billing, and accounting handled manually.",
        "F&A system in use, but only for recording cash collections — no demand or accrual accounting.",
        "Revenue modules implemented for property tax (and other user charges), but not integrated with F&A.",
        "Revenue modules implemented (PT at least) & integrated with F&A for accrual entries and collection posting.",
        "Revenue modules (PT at least) fully integrated with F&A for continuous accrual posting through the year, with collection and reconciliation.",
    ],
    "q07": [
        "Less than 20%",
        "21% to 40%",
        "41% to 60%",
        "61% to 80%",
        "More than 80%",
    ],
    "q08": [
        "No TP collection or Payment Gateway integration exist",
        "TP Collection Exist as a disparate process",
        "Payment Gateway integration exist with manual tracking & accounting",
        "Integration with TP collection and Payment Gateway with manual reconciliation",
        "Real-time end to end process integration with auto-reconciliation",
    ],
    "q09": [
        "Not captured. No formal process in place",
        "Captured in registers, no audit trail",
        "Captured in system with basic logs",
        "Captured with two-level approval and full audit trail",
        "Tamper-evident logs with system-enforced reason codes",
    ],
    "q10": [
        "Expenses recorded only on payment; no liability recognition.",
        "Pending bills/liabilities tracked outside accounts; books remain cash-based.",
        "Cash basis payment processing through the year; outstanding liabilities recognised manually at year-end for AFS.",
        "Liabilities computed from source systems but posted manually as accrual entries.",
        "Full accrual. Liability accounted automatically as per NMAM; Bill linked payment recorded as settlement.",
    ],
    "q11": [
        "No System Used - Purchase orders, work orders, bill verification, and payment vouchers are all processed manually on paper",
        "A financial accounting system is in use, but only to record payment vouchers.",
        "Modules for purchase orders, works/contracts, or payroll are in use and generate bills/claims digitally, but these are not integrated with the F&A system.",
        "Purchase/works/payroll modules are integrated with F&A — bill verification automatically generates an accrual entry",
        "The entire expenditure cycle — PO/WO, bill verification, accrual posting, payment, and reconciliation - is processed on an integrated platform with real-time visibility",
    ],
    "q12": [
        "Manual computation & Manual remittance",
        "Spreadsheet-supported computation and remittance",
        "System-computed at voucher level and manual remittance",
        "System-computed with auto-challan generation and remittance",
        "End-to-end automation including return filing and reconciliation",
    ],
    "q13": [
        "No linkage between project execution and contractor payment",
        "Manual linkage via measurement books and reference numbers",
        "System linkage to measurement-book entries",
        "Integrated works management module drives Bill generation/Payment processing",
        "Integrated works with outcome- and milestone-linked payments with geo-tagged proof",
    ],
    "q14": [
        "No asset register maintained.",
        "Asset register maintained separately; not linked to accounting.",
        "Capitalized asset register linked to accounting; lifecycle updates handled manually.",
        "Assets comprehensively accounted for with systematic register maintenance and regular lifecycle accounting.",
        "Complete lifecycle-based asset accounting with fully reconciled books and asset records.",
    ],
    "q15": [
        "No Asset module. Assets maintained manually outside the accounting system.",
        "Asset module used for asset register maintenance. Accounting of lifecycle-updates is manual.",
        "Asset module integrated with F&A module for capitalization. Accounting of lifecycle-updates is manual.",
        "Procurement and asset lifecycle integrated with automatic accounting entries.",
        "Fully automated asset lifecycle with real-time valuation, NMAM-compliant accounting, and integrated reporting.",
    ],
    "q16": [
        "Cash basis accounting only.",
        "Limited application of accrual principles; predominantly cash-based.",
        "Accrual financial statements prepared through year-end adjustments.",
        "Books substantially maintained on an accrual basis as per NMAM.",
        "Accrual principles fully embedded across all accounting processes and financial reporting.",
    ],
    "q17": [
        "Traditional ledger/object heads; NMAM CoA not adopted.",
        "Partial mapping to NMAM CoA; legacy classifications used for Function, Department.",
        "NMAM CoA implemented with some manual mapping or exceptions.",
        "Complete NMAM CoA consistently applied across accounting and reporting.",
        "Fully aligned multi-dimensional NMAM CoA supporting budgeting, accounting, reporting, and interoperability standards.",
    ],
    "q18": [
        "No digital Finance & Accounting system.",
        "Standalone accounting system with manual ledger entries.",
        "Integrated with revenue and expenditure modules for automatic ledger posting.",
        "Integrated across the complete financial lifecycle, including budgeting.",
        "Interoperable F&A system integrated with external government platforms and standardized data exchange.",
    ],
    "q19": [
        "Done outside system on spreadsheet",
        "Done in system but post year-end, with manual reversals",
        "Done in system with documented workflow",
        "System-driven with approval workflow and audit trail",
        "System-driven, NMAM-compliant, audit-ready with auto-disclosures",
    ],
    "q20": [
        "Not formally tracked",
        "Tracked in registers",
        "Tracked in F&A but reconciled periodically",
        "System-driven with auto-accounting",
        "Fully system-driven with reconciliation support and real-time consolidation",
    ],
    "q21": [
        "Carried forward manually without verification",
        "Verified manually through records and reconciliation",
        "System-generated with manual verification",
        "System-generated and reconciled before budget preparation",
        "Automatically derived from approved closing balances and integrated into budgeting",
    ],
    "q22": [
        "No Digital Accounting System Exist",
        "20% to 40%",
        "41% to 60%",
        "61% to 80%",
        "More than 80%",
    ],
    "q23": [
        "Less than 25%",
        "26% to 50%",
        "51% to 75%",
        "76% to 95%",
        "All departments on one platform",
    ],
    "q24": [
        "Legacy, no active support",
        "Minimal support, single vendor",
        "Active vendor support, no in-house team",
        "Active vendor plus in-house product team",
        "Active in-house team plus state-level shared service",
    ],
    "q25": [
        "Master data maintained independently with no central governance.",
        "Master data maintained separately within individual systems.",
        "Core master data centrally managed for the F&A system.",
        "Centrally governed master data shared across all internal modules.",
        "Enterprise-wide master data with real-time synchronization and interoperability across systems.",
    ],
    "q26": [
        "No defined data standards are followed. Data structures vary across departments, and information exchange is largely manual.",
        "Basic digital systems exist, but data formats and structures are specific to individual applications with limited standardization.",
        "Standardized data structures and Chart of Accounts are adopted within the system, enabling consistent reporting and internal data sharing.",
        "The system follows common standards and supports automated data exchange with external government systems such as IFMS, PFMS, banking, or service delivery platforms.",
        "The system adopts open and recognized data standards, enabling seamless interoperability, real-time information exchange, consolidated reporting, and reuse of data across the ecosystem.",
    ],
    "q27": [
        "No APIs; data shared manually.",
        "Data shared through file exports/imports only.",
        "APIs available for selected integrations.",
        "Standardized APIs support automated integration with internal and external systems.",
        "Real-time interoperable API ecosystem supporting government platforms, dashboards, and open data.",
    ],
    "q28": [
        "None",
        "Informal, in vendor documentation only",
        "Documented but not maintained",
        "Documented and version-controlled",
        "Public, machine-readable, aligned to national standards",
    ],
    "q29": [
        "No formal training",
        "Initial training only, no refresh",
        "Periodic refresher training",
        "Structured training plan with certification",
        "Continuous learning, role-based curriculum, integrated with iGOT",
    ],
    "q30": [
        "There is no designated owner within the State/ULB. The system is largely managed by the vendor or external agency, with limited internal understanding or oversight.",
        "A finance or accounts officer is designated as the point of contact, but system administration, issue resolution, and enhancements depend primarily on external support.",
        "Clear ownership exists within the finance department, with defined responsibilities for system usage, data quality, and coordination with implementation partners.",
        "The State/ULB has dedicated functional and/or IT personnel responsible for system administration, configuration management, user support, training, and vendor coordination.",
        "The State/ULB has clear ownership of the F&A system, with dedicated internal capacity, defined processes, regular training, and the ability to manage the system effectively",
    ],
    "q31": [
        "There are no defined security rules for data, users, or systems.",
        "Basic login controls exist, but security practices are informal and inconsistently followed.",
        "Security policies and user access controls are defined and regularly applied.",
        "Security is actively managed through access reviews, monitoring, backups, and audit trails.",
        "Security is continuously monitored and governed through formal policies, audits, risk management, and disaster recovery processes.",
    ],
    "q32": [
        "Ad-hoc, project-mode and unplanned budget",
        "Annual budget line item in O&M, sometimes underfunded",
        "Stable annual budget line, adequate",
        "Multi-year MoU-backed funding",
        "Embedded in state-level shared service with assured funding",
    ],
    "q33": [
        "Prepared manually outside system",
        "Partially system-supported, manual reconciliation",
        "System-generated, manual review",
        "System-generated and reviewed within system, audit-ready",
        "System-generated, audit-ready, published on standardised portal",
    ],
    "q34": [
        "More than 18 months",
        "12 to 18 months",
        "8 to 11 months",
        "4 to 7 months",
        "Within 3 months",
    ],
    "q35": [
        "Not submitted in NMAM format",
        "Submitted, partial NMAM compliance",
        "Submitted in NMAM format, manual conversion",
        "Submitted in NMAM format, system-generated",
        "Submitted in NMAM format, machine-readable, system-validated",
    ],
    "q36": [
        "Not published",
        "Published in annual report only",
        "Published on website, updated annually",
        "Dashboard on website, updated quarterly",
        "Open data portal, machine-readable, updated monthly or more frequently",
    ],
    "q37": [
        "No logging",
        "Partial logging in some modules",
        "Logging across modules, queried on demand",
        "Comprehensive logging with searchable audit trail",
        "Tamper-evident logging with role-based query and retention policies",
    ],
    "q38": [
        "No system access, audit through documents",
        "Read-only export provided on request",
        "Auditor login with restricted modules",
        "Audit workbench within system with full trail access",
        "Self-service auditor portal with secure remote access",
    ],
    "q39": [
        "Tracked outside system",
        "Tracked in spreadsheets",
        "Tracked in system, periodic updates",
        "Tracked in system with workflow",
        "Closed-loop with auto-flagging and reporting",
    ],
    "q40": [
        "Manual, periodic",
        "Mostly manual, some system support",
        "System-driven with manual exceptions",
        "Auto-reconciled with exception workflow",
        "Real-time auto-reconciliation with anomaly alerts",
    ],
} %}

{% set mapping_header =
    "ULB Code (as per City Finance Portal; Please double-check the ULB Code to ensure your response is correctly mapped and processed)"
%}

{% set resolved_columns = [] %}
{% set mapping_matches = [] %}
{% set missing_columns = [] %}
{% set ambiguous_columns = [] %}

{% if execute %}

    {% set source_columns = adapter.get_columns_in_relation(nmam_relation) %}
    {% set available_column_names = [] %}

    {% for column in source_columns %}
        {% set actual_name = column.name | trim %}
        {% set actual_normalized = modules.re.sub('[^a-z0-9]+', ' ', actual_name | lower) | trim %}
        {% set expected_mapping_normalized = modules.re.sub('[^a-z0-9]+', ' ', mapping_header | lower) | trim %}

        {% do available_column_names.append(actual_name) %}

        {#
          Supports:
          - original Google Form headers;
          - PostgreSQL-truncated headers;
          - snake_case/sanitised headers.
        #}
        {% if
            actual_normalized == expected_mapping_normalized
            or expected_mapping_normalized.startswith(actual_normalized)
            or actual_normalized.startswith(expected_mapping_normalized)
            or (
                'ulb' in actual_normalized
                and 'code' in actual_normalized
                and 'city' in actual_normalized
                and 'finance' in actual_normalized
            )
        %}
            {% do mapping_matches.append(actual_name) %}
        {% endif %}
    {% endfor %}

    {% for requested_column in requested_columns %}
        {% set expected_name = requested_column[0] %}
        {% set output_alias = requested_column[1] %}
        {% set expected_normalized = modules.re.sub('[^a-z0-9]+', ' ', expected_name | lower) | trim %}
        {% set column_matches = [] %}

        {% for column in source_columns %}
            {% set actual_name = column.name | trim %}
            {% set actual_normalized = modules.re.sub('[^a-z0-9]+', ' ', actual_name | lower) | trim %}

            {% if
                actual_normalized == expected_normalized
                or expected_normalized.startswith(actual_normalized)
                or actual_normalized.startswith(expected_normalized)
            %}
                {% do column_matches.append(actual_name) %}
            {% endif %}
        {% endfor %}

        {% if column_matches | length == 0 %}
            {% do missing_columns.append(expected_name) %}
        {% elif column_matches | length > 1 %}
            {% do ambiguous_columns.append(
                expected_name ~ ' => ' ~ (column_matches | join(', '))
            ) %}
        {% else %}
            {% do resolved_columns.append([column_matches[0], output_alias]) %}
        {% endif %}
    {% endfor %}

    {% if mapping_matches | length == 0 %}
        {{ exceptions.raise_compiler_error(
            'NMAM mapping column was not found in '
            ~ nmam_relation
            ~ '. Available columns are: '
            ~ (available_column_names | join(' | '))
        ) }}
    {% elif mapping_matches | length > 1 %}
        {{ exceptions.raise_compiler_error(
            'Multiple possible NMAM mapping columns were found: '
            ~ (mapping_matches | join(' | '))
        ) }}
    {% endif %}

    {% if missing_columns | length > 0 %}
        {{ exceptions.raise_compiler_error(
            'The following requested NMAM columns were not found: '
            ~ (missing_columns | join(' | '))
            ~ '. Available columns are: '
            ~ (available_column_names | join(' | '))
        ) }}
    {% endif %}

    {% if ambiguous_columns | length > 0 %}
        {{ exceptions.raise_compiler_error(
            'The following requested NMAM columns matched more than once: '
            ~ (ambiguous_columns | join(' | '))
        ) }}
    {% endif %}

    {% set mapping_column = mapping_matches[0] %}

{% else %}

    {# Used only by dbt parse when database metadata is unavailable. #}
    {% set mapping_column = mapping_header %}

    {% for requested_column in requested_columns %}
        {% do resolved_columns.append([requested_column[0], requested_column[1]]) %}
    {% endfor %}

{% endif %}

WITH states AS (
    SELECT
        _id AS state_id,
        name AS state_name
    FROM {{ source('cityfinance_prod', 'states') }}
    WHERE "isUT" = 'false'
),

ulb_types AS (
    /* Resolve the ulbs.ulbType object ID to the ULB type name. */
    SELECT
        BTRIM(_id::TEXT, ' "') AS ulb_type_id,
        MAX(name) AS ulb_type_name
    FROM {{ source('market_readiness', 'ulbtypes') }}
    WHERE "isActive" = 'true'
    GROUP BY BTRIM(_id::TEXT, ' "')
),

iso_codes AS (
    /* Aggregate defensively so the ISO join cannot duplicate a ULB row. */
    SELECT
        LOWER(BTRIM(state::TEXT)) AS state_join_key,
        MAX(iso_code) AS iso_code
    FROM {{ source('cityfinance_prod', 'iso_codes') }}
    GROUP BY LOWER(BTRIM(state::TEXT))
),

ulb_master AS (
    /* Base population: one row per active, published non-UT ULB.
       Use censusCode when available; otherwise fall back to sbCode. */
    SELECT
        u._id AS ulb_id,
        u.code AS ulb_code,
        s.state_name,
        u.name AS ulb_name,
        ut.ulb_type_name AS "ulbType",
        i.iso_code,
        COALESCE(
            NULLIF(BTRIM(u."censusCode"::TEXT, ' "'), ''),
            NULLIF(BTRIM(u."sbCode"::TEXT, ' "'), '')
        ) AS census_code
    FROM {{ source('cityfinance_prod', 'ulbs') }} u

    INNER JOIN states s
        ON u.state = s.state_id

    LEFT JOIN ulb_types ut
        ON BTRIM(u."ulbType"::TEXT, ' "') = ut.ulb_type_id

    LEFT JOIN iso_codes i
        ON LOWER(BTRIM(s.state_name::TEXT)) = i.state_join_key

    WHERE
        u."isActive" = 'true'
        AND u."isPublish" = 'true'
),

nmam_timestamp_raw AS (
    /*
      Normalize the source timestamp as text. This supports timestamp values
      stored as plain text, JSON-style quoted text, or PostgreSQL timestamps.
      Example supported value: 2026-08-07T17:21:57
    */
    SELECT
        n.*,
        NULLIF(
            REGEXP_REPLACE(
                n.{{ adapter.quote(resolved_columns[0][0]) }}::TEXT,
                '^[[:space:]"]+|[[:space:]"]+$',
                '',
                'g'
            ),
            ''
        ) AS response_timestamp_raw
    FROM {{ nmam_relation }} n
),

nmam_timestamp_extracted AS (
    /*
      Extract the date-time from anywhere in the source value instead of
      requiring the value to begin at the first character. This avoids NULL
      results caused by quotes, JSON wrappers, or invisible whitespace.
    */
    SELECT
        n.*,
        SUBSTRING(
            n.response_timestamp_raw
            FROM '[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}'
        ) AS iso_timestamp_text,
        SUBSTRING(
            n.response_timestamp_raw
            FROM '[0-9]{2}/[0-9]{2}/[0-9]{4}[[:space:]]+[0-9]{2}:[0-9]{2}:[0-9]{2}'
        ) AS dmy_timestamp_text
    FROM nmam_timestamp_raw n
),

nmam_timestamp_parsed AS (
    SELECT
        n.*,
        CASE
            /* ISO-8601: 2026-08-07T17:21:57 */
            WHEN n.iso_timestamp_text IS NOT NULL
            THEN REPLACE(
                    n.iso_timestamp_text,
                    'T',
                    ' '
                 )::TIMESTAMP

            /* Google Form display format: 07/08/2026 17:21:57 */
            WHEN n.dmy_timestamp_text IS NOT NULL
            THEN TO_TIMESTAMP(
                    REGEXP_REPLACE(
                        n.dmy_timestamp_text,
                        '[[:space:]]+',
                        ' ',
                        'g'
                    ),
                    'DD/MM/YYYY HH24:MI:SS'
                 )::TIMESTAMP

            ELSE NULL
        END AS response_timestamp_parsed
    FROM nmam_timestamp_extracted n
),

nmam_prepared AS (
    SELECT
        n.*,
        n.response_timestamp_parsed::DATE AS response_date_parsed
    FROM nmam_timestamp_parsed n
),
nmam_ranked AS (
    /* Keep only the latest NMAM response per mapped census code. */
    SELECT
        NULLIF(
            BTRIM(n.{{ adapter.quote(mapping_column) }}::TEXT, ' "'),
            ''
        ) AS nmam_census_code,
{% for resolved_column in resolved_columns %}
        n.{{ adapter.quote(resolved_column[0]) }}
            AS {{ adapter.quote(resolved_column[1]) }},{% endfor %}
        n.response_timestamp_parsed,
        n.response_date_parsed,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(
                BTRIM(n.{{ adapter.quote(mapping_column) }}::TEXT, ' "'),
                ''
            )
            ORDER BY
                n.response_timestamp_parsed DESC NULLS LAST
        ) AS response_rank
    FROM nmam_prepared n
    WHERE NULLIF(
        BTRIM(n.{{ adapter.quote(mapping_column) }}::TEXT, ' "'),
        ''
    ) IS NOT NULL
),

nmam_one_response AS (
    SELECT
        nmam_census_code,
{% for resolved_column in resolved_columns %}
        {{ adapter.quote(resolved_column[1]) }},{% endfor %}
        response_timestamp_parsed,
        response_date_parsed
    FROM nmam_ranked
    WHERE response_rank = 1
)

SELECT
    u.ulb_code,
    u.state_name,
    u.ulb_name,
    u."ulbType",
    u.iso_code,
    u.census_code,
    CASE
        WHEN n.nmam_census_code IS NOT NULL THEN 1
        ELSE 0
    END AS "Availability",
    
    CASE
        when u.census_code IS NOT NULL THEN 1
        ELSE 0
    END AS "Total_Availability",

    CASE
        WHEN n.nmam_census_code IS NULL THEN NULL
        WHEN n.response_date_parsed IS NULL THEN 'Invalid Timestamp'
        WHEN n.response_date_parsed
             >= (NOW() AT TIME ZONE 'Asia/Kolkata')::DATE
            THEN 'Future'
        WHEN n.response_date_parsed
             = ((NOW() AT TIME ZONE 'Asia/Kolkata')::DATE - 1)
            THEN 'Yesterday'
        WHEN n.response_date_parsed
             >= DATE_TRUNC(
                    'week',
                    NOW() AT TIME ZONE 'Asia/Kolkata'
                )::DATE
         AND n.response_date_parsed
             <= (NOW() AT TIME ZONE 'Asia/Kolkata')::DATE
            THEN 'This Week'
        WHEN n.response_date_parsed
             >= (
                    DATE_TRUNC(
                        'week',
                        NOW() AT TIME ZONE 'Asia/Kolkata'
                    ) - INTERVAL '1 week'
                )::DATE
         AND n.response_date_parsed
             < DATE_TRUNC(
                    'week',
                    NOW() AT TIME ZONE 'Asia/Kolkata'
                )::DATE
            THEN 'Last Week'
        ELSE 'Older'
    END AS "Update_Period",
{% for resolved_column in resolved_columns %}
{% set output_alias = resolved_column[1] %}
{% if output_alias in question_options %}
    CASE
        WHEN n.{{ adapter.quote(output_alias) }} IS NULL
          OR NULLIF(
                BTRIM(
                    n.{{ adapter.quote(output_alias) }}::TEXT,
                    ' "'
                ),
                ''
             ) IS NULL
            THEN NULL
{% for option in question_options[output_alias] %}
{% set normalized_option = modules.re.sub(
    '[^a-z0-9]+',
    '',
    option | lower
) %}
        WHEN REGEXP_REPLACE(
                LOWER(
                    BTRIM(
                        n.{{ adapter.quote(output_alias) }}::TEXT,
                        ' "'
                    )
                ),
                '[^a-z0-9]+',
                '',
                'g'
             ) = '{{ normalized_option | replace("'", "''") }}'
            THEN '{{ option | replace("'", "''") }}'
{% endfor %}
        ELSE 'Other'
    END AS {{ adapter.quote(output_alias) }}{% if not loop.last %},{% endif %}
{% else %}
    n.{{ adapter.quote(output_alias) }}
        AS {{ adapter.quote(output_alias) }}{% if not loop.last %},{% endif %}
{% endif %}
{% endfor %}
FROM ulb_master u

LEFT JOIN nmam_one_response n
    ON NULLIF(BTRIM(u.census_code::TEXT), '') = n.nmam_census_code