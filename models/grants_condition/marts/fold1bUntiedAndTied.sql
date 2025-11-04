{{ config(
    materialized='table',
    tags=['grants_condition', 'marts']
) }}

-- ============================================================================
-- Model: fold1bUntiedAndTied
-- Purpose:
--   Calculates compliance and eligibility metrics for all active, non-million-plus ULBs,
--   at the ULB and year level, including property tax growth, account status,
--   DUR submissions, baseline submissions, and grant eligibility.
-- ============================================================================

with active_ulbs as (
    -- Select all active ULBs that are not million-plus cities
    select
        _id as ulb_id,
        name,
        state
    from {{ source('cityfinance_prod','ulbs') }} 
    where "isActive" = 'true'
      and "isMillionPlus" = 'No'
),

states as (
    -- Select all states except UTs and test states
    select
        _id as state_id,
        name as state_name
    from {{ source('cityfinance_prod','states') }} 
    where "isUT" = 'false'
      and name != 'TEST STATE'
),

iso_codes as (
    -- ISO codes for states
    select
        state,
        iso_code
    from {{ source('cityfinance_prod','iso_codes') }}
),

years as (
    -- All years from the years table
    select
        _id as year_id,
        year
    from {{ source('cityfinance_prod','years') }}
),

-- ============================================================================
-- 🧠 Explanation of `ulb_years` CTE
-- ============================================================================

-- ✅ Purpose:
--   Generates a complete matrix of all combinations of active, eligible ULBs and
--   all financial years present in the dataset. This acts as the foundational
--   scaffold for joining other datasets like property tax, annual accounts, DUR, etc.

-- ============================================================================
-- 🔍 Key Inputs:
-- ----------------------------------------------------------------------------
-- - `active_ulbs`: All active ULBs that are eligible for grants (non-million-plus)
-- - `years`: All available financial years from the master `years` table

-- ============================================================================
-- 📐 Logic:
-- ----------------------------------------------------------------------------
-- - Performs a `CROSS JOIN` between `active_ulbs` and `years`
-- - Ensures that every active ULB is evaluated for every financial year
-- - Each row represents a unique (ULB, Year) pair

-- ============================================================================
-- 🏁 Output Columns:
-- ----------------------------------------------------------------------------
-- | Column            | Description                         |
-- |-------------------|-------------------------------------|
-- | ulb_id            | Unique ID of the ULB                |
-- | ulb_name          | Name of the ULB                     |
-- | state             | State ID to which ULB belongs       |
-- | design_year_id    | ID of the financial year            |
-- | design_year       | Year string in 'YYYY-YY' format     |

-- ============================================================================
-- ✅ Why this approach?
-- ----------------------------------------------------------------------------
-- - Guarantees full ULB-year coverage even if data is missing in some datasets
-- - Ensures consistent structure for left joins across all derived tables
-- - Avoids null mismatches and incomplete joins downstream

-- ============================================================================

ulb_years as (
    -- Create a matrix of all active ULBs and all years
    select
        u.ulb_id,
        u.name as ulb_name,
        u.state,
        y.year_id as design_year_id,
        y.year as design_year
    from active_ulbs u
    cross join years y
),

-- ============================================================================
-- 🧠 Explanation of `annual_accounts` CTE
-- ============================================================================

-- ✅ Purpose:
--   Determines the submission status of annual accounts (Provisional and Audited)
--   for each ULB and design year. Ensures complete coverage of all ULB-year
--   combinations and avoids nulls by defaulting to 'No' when no record exists.

-- ============================================================================
-- 🔍 Key Inputs:
-- ----------------------------------------------------------------------------
-- - `ulb_years`: All valid ULB and year combinations
-- - `annualaccountdatas`: Source table containing annual accounts submission info

-- ============================================================================
-- 📐 Logic:
-- ----------------------------------------------------------------------------
-- - Left join `annualaccountdatas` on ULB ID and design year
-- - For each ULB-year:
--     1. If no record exists → set submission status to `'No'`
--     2. If record exists and:
--        - `currentFormStatus` = 4 or 6
--        - AND `unAudited.submit_annual_accounts` = true → `'Yes'` for Provisional
--        - AND `audited.submit_annual_accounts` = true → `'Yes'` for Audited
--     3. Else → `'No'`

-- ============================================================================
-- 🏁 Output Columns:
-- ----------------------------------------------------------------------------
-- | Column                         | Description                                     |
-- |--------------------------------|-------------------------------------------------|
-- | ulb                            | ULB ID from `ulb_years`                         |
-- | design_year                    | Design year ID from `ulb_years`                 |
-- | "Annual Accounts Provisional" | 'Yes' if provisional accounts submitted properly |
-- | "Annual Accounts Audited"     | 'Yes' if audited accounts submitted properly     |

-- ============================================================================
-- ✅ Why this approach?
-- ----------------------------------------------------------------------------
-- - Prevents nulls in downstream joins or filters
-- - Eliminates need for coalesce() or defensive logic later
-- - Ensures consistent 'Yes'/'No' values across the model

-- ============================================================================

annual_accounts as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,

        -- Annual Accounts Provisional Submission
        case
            when aa.ulb is null then 'No'  -- No record at all
            when aa."currentFormStatus" in (4, 6)
                 and (aa."unAudited"->>'submit_annual_accounts')::boolean = true
            then 'Yes'
            else 'No'
        end as "Annual Accounts Provisional",

        -- Annual Accounts Audited Submission
        case
            when aa.ulb is null then 'No'  -- No record at all
            when aa."currentFormStatus" in (4, 6)
                 and (aa."audited"->>'submit_annual_accounts')::boolean = true
            then 'Yes'
            else 'No'
        end as "Annual Accounts Audited"

    from ulb_years uy
    left join {{ source('cityfinance_prod','annualaccountdatas') }} aa
        on uy.ulb_id = aa.ulb and uy.design_year_id = aa.design_year
),

-- ============================================================================
-- 🧠 Explanation of `state_gsdp` CTE
-- ============================================================================

-- ✅ Purpose:
--   Extracts the state-wise GSDP (Gross State Domestic Product) value for each
--   state and financial year, used for evaluating property tax collection growth
--   eligibility for ULBs. The GSDP value is matched to the relevant financial
--   year by joining the `designYear` field in the `data` array with the
--   `design_year_id` from the master years table.

-- ============================================================================
-- 🔍 Key Inputs:
-- ----------------------------------------------------------------------------
-- - `state_gsdp`: Source table containing economic data for each state,
--   where the `data` field is a JSON array of GSDP records for multiple years.
--   Each element contains:
--     - `designYear`: Year ID (foreign key to years table)
--     - `currentPrice`: GSDP value for that year (numeric)

-- ============================================================================
-- 📐 Logic:
-- ----------------------------------------------------------------------------
-- - Unnest the `data` JSON array for each state.
-- - For each element, extract the `designYear` and `currentPrice`.
-- - Output one row per state and design year, with the corresponding GSDP value.
-- - This enables precise year-wise matching for downstream eligibility checks.
-- - If no GSDP value exists for a given state-year, the join will result in NULL.

-- ============================================================================
-- 🏁 Output Columns:
-- ----------------------------------------------------------------------------
-- | Column                      | Description                                   |
-- |-----------------------------|-----------------------------------------------|
-- | stateId                     | Unique ID of the state                        |
-- | design_year_id              | Year ID (matches years._id / ulb_years.design_year_id) |
-- | "Property tax State GSDP"   | GSDP value (current price, numeric, rounded)  |

-- ============================================================================
-- ✅ Why this approach?
-- ----------------------------------------------------------------------------
-- - Ensures accurate, year-specific GSDP comparison for each ULB and year.
-- - Supports eligibility logic that depends on the correct GSDP benchmark.
-- - Handles missing data gracefully by returning NULL if no match is found.

-- ============================================================================

state_gsdp as (
    -- Unnest data array and match designYear to year_id
    select
        sg."stateId",
        d->>'designYear' as design_year_id,
        round((d->>'currentPrice')::numeric, 2) as "Property tax State GSDP"
    from {{ source('cityfinance_prod','state_gsdp') }} sg,
         lateral jsonb_array_elements(sg.data) as d
),

-- ============================================================================
-- 🧠 Explanation of `property_tax_submitted` CTE
-- ============================================================================

-- ✅ Purpose:
--   Determines whether each ULB has submitted its property tax form for a given
--   financial year, ensuring that missing records are treated safely.

-- ============================================================================
-- 🔍 Key Inputs:
-- ----------------------------------------------------------------------------
-- - `ulb_years`: Complete list of ULB and design year combinations
-- - `propertytaxops`: Source table indicating form submission status per ULB and year

-- ============================================================================
-- 📐 Logic:
-- ----------------------------------------------------------------------------
-- - Left join `propertytaxops` to ensure all ULB-year pairs are retained
-- - If no record exists for a ULB-year → return `'No'`
-- - If a record exists and `currentFormStatus` is 4 or 6 → return `'Yes'`
-- - Otherwise → return `'No'`

-- ============================================================================
-- 🏁 Output Columns:
-- ----------------------------------------------------------------------------
-- | Column                  | Description                                 |
-- |-------------------------|---------------------------------------------|
-- | ulb                    | ULB ID from `ulb_years`                      |
-- | design_year            | Design year ID from `ulb_years`             |
-- | "Property tax Submitted"| 'Yes' if valid form submitted, else 'No'    |

-- ============================================================================
-- ✅ Why this approach?
-- ----------------------------------------------------------------------------
-- - Ensures downstream models never encounter null values
-- - Prevents incorrect eligibility calculations due to missing form records
-- - Promotes consistency across all eligibility condition logic

-- ============================================================================

property_tax_submitted as (
    -- Ensures all ULB-year combinations are covered
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year,
        case
            when pt.ulb is null then 'No'                            -- No record at all
            when pt."currentFormStatus" in (4, 6) then 'Yes'         -- Valid form submitted
            else 'No'                                                -- Record exists but not eligible
        end as "Property tax Submitted"
    from ulb_years uy
    left join {{ source('cityfinance_prod','propertytaxops') }} pt
        on uy.ulb_id = pt.ulb and uy.design_year_id = pt.design_year
),

-- ============================================================================
-- 🧠 Explanation of `property_tax_mapper` CTE
-- ============================================================================

-- ✅ Purpose:
--   This Common Table Expression (CTE) extracts **property tax collection values**
--   for all ULBs across financial years from the `propertytaxopmappers` table,
--   specifically for records marked with displayPriority `'1.17'`.

-- ============================================================================
-- 🔍 Why displayPriority = '1.17'?
-- ============================================================================
-- The `propertytaxopmappers` table can store multiple financial metrics.
-- The field `"displayPriority"` acts like a key to distinguish them.
--   - `1.17` corresponds specifically to "Property Tax Collection" data.
-- ✅ This ensures we're only picking the correct financial indicator for analysis.

-- ============================================================================
-- 🧹 Numeric Value Cleaning Using Regex
-- ============================================================================
-- The line:
--     when ptm.value ~ '^(-?(\d+(.\d*)?|.\d+))$' then ptm.value::numeric
-- ensures only clean, numeric values are considered for further computation.
--
-- Breakdown of regex '^(-?(\d+(.\d*)?|.\d+))$':
--   - ^ and $    : Anchor the match to the entire string
--   - -?         : Optional minus sign (allows negative numbers)
--   - \d+        : One or more digits (e.g., "123")
--   - (.\d*)?    : Optional dot followed by zero or more digits (allows "123." and "123.45")
--   - |\.\d+     : Or a leading dot followed by one or more digits (allows ".45")
--
-- Matches (examples): "123", "0", "12.34", "0.4", ".4", "123.", "-1", "-.5", "001.20"
-- Does NOT match: "+1" (plus sign), "1,234.56" (commas), "" (empty string), "   " (only whitespace), "abc"
--
-- Important note:
--   In some SQL regex implementations '.' is a wildcard. To strictly match a literal decimal
--   point and avoid unintended matches, prefer the escaped/literal-dot variant:
--     '^(-?(?:\d+(?:\.\d*)?|\.\d+))$'
--
-- This prevents SQL casting errors when converting to numeric and explicitly treats
-- empty/whitespace/non-numeric strings as NULL.
--
-- This prevents SQL casting errors when converting to `numeric`.

-- ============================================================================
-- 📆 Year Mapping and Filtering
-- ============================================================================
-- The table is left-joined to `years` to fetch:
--   - `y.year` → Financial year string (e.g., `'2022-23'`)
--   - `y._id`  → Unique year ID to assist in downstream joins

-- The optional clause:
--     and y.year ~ '^\d{4}-\d{2}$'
-- is used to **filter out malformed year entries** in the `years` table.
--
-- Regex `'^\d{4}-\d{2}$'` breakdown:
--   - `\d{4}`  → 4 digits (e.g., `2022`)
--   - `-`      → A dash
--   - `\d{2}`  → 2 digits (e.g., `23`)
--
-- ✅ Matches: `2022-23`, `2023-24`
-- ❌ Rejects: `FY2023`, `2023/24`, `TestYear`, etc.
--
-- 🔸 This is a **safety net** in case the `years` table contains junk data.
-- 🔸 It is **optional** if you're confident that the data is clean and standardized.

-- ============================================================================
-- 📦 Final Output Columns from this CTE:
-- ============================================================================
-- | Column        | Description                              |
-- | ------------- | ---------------------------------------- |
-- | ptm.ulb       | ULB ID                                   |
-- | y.year_string | Financial year string (e.g., '2022-23')  |
-- | y.year_id     | Corresponding year ID (used in joins)    |
-- | value         | Property tax amount as numeric           |
--
-- ✅ This structured and cleaned output is later used for:
--   - Calculating growth between years (`growth_values` CTE)
--   - Checking eligibility for grants based on tax performance

property_tax_mapper as (
    -- Property tax collection value for each ULB and year (displayPriority 1.17)
    select
        ptm.ulb,
        y.year as year_string,
        y._id as year_id,
        case 
            when ptm.value ~ '^(-?(\d+(.\d*)?|.\d+))$' then ptm.value::numeric
            else null
        end as value
    from {{ source('cityfinance_prod','propertytaxopmappers') }} ptm
    left join {{ source('cityfinance_prod','years') }} y
        on ptm.year = y._id
    where ptm."displayPriority" = '1.17'
      --and y.year ~ '^\d{4}-\d{2}$'
),

-- ============================================================================
-- 🧠 Explanation of `growth_values` CTE
-- ============================================================================

-- ✅ Purpose:
--   Calculates property tax growth metrics for each ULB and design year,
--   and determines if a ULB is eligible under the "Property Tax Growth Condition"
--   as part of grant eligibility.

-- ============================================================================
-- 🔍 Key Inputs:
-- ----------------------------------------------------------------------------
-- - `ulb_years`: All ULB-year combinations (active, non-million-plus ULBs)
-- - `property_tax_mapper`: Contains property tax collection values (priority 1.17)
-- - `property_tax_submitted`: Whether the ULB submitted its property tax form
-- - `state_gsdp`: GSDP growth values for each state
-- - `years`: Used to determine T-1 and T-2 financial years for the ULB

-- ============================================================================
-- 📐 Computation Logic:
-- ----------------------------------------------------------------------------
-- - `value_A`: Property tax collection in Year T-1 (1 year before design year)
-- - `value_B`: Property tax collection in Year T-2 (2 years before design year)
-- - `growth_rate`: ((A - B) / B) * 100
-- - `"Eligible for Property Tax Condition"` is set to:
--     - 'Yes' if:
--         - Property Tax was submitted (`"Property tax Submitted"` = 'Yes')
--         - Both `value_A` and `value_B` are valid (numeric, not null, B ≠ 0)
--         - Growth rate > state's GSDP value
--     - 'No' otherwise

-- ============================================================================
-- 🏁 Final Output Columns:
-- ----------------------------------------------------------------------------
-- | Column                            | Description                                 |
-- |-----------------------------------|---------------------------------------------|
-- | ulb_id                            | ULB identifier                               |
-- | design_year                       | Target financial year                        |
-- | "Property tax State GSDP"         | GSDP benchmark for the state                 |
-- | value_A                           | Property tax value for T-1                   |
-- | value_B                           | Property tax value for T-2                   |
-- | value_A_year                      | Year string of T-1 (e.g., '2023-24')         |
-- | value_B_year                      | Year string of T-2 (e.g., '2022-23')         |
-- | "Property tax Submitted"          | Submission status from property_tax_submitted |
-- | "Eligible for Property Tax Condition" | Precomputed eligibility (Yes/No)           |

-- ============================================================================
-- ✅ Why Precompute Eligibility?
-- ----------------------------------------------------------------------------
-- - Avoids repeating the same conditional logic in the final SELECT
-- - Keeps the model clean and modular
-- - Easy to debug and reuse eligibility flag in multiple conditions

-- ============================================================================

growth_values as (
    select
        uy.ulb_id,
        uy.design_year,
        g."Property tax State GSDP",
        ptm_A.value as value_A,  -- T-1 value
        ptm_B.value as value_B,   -- T-2 value
        y_A.year as value_A_year,
        y_B.year as value_B_year,
        
        -- Join with property tax submission
        pts."Property tax Submitted",

        -- Central eligibility check
        case
            when pts."Property tax Submitted" = 'Yes'
                 and ptm_B.value is not null and ptm_B.value != 0
                 and ((ptm_A.value - ptm_B.value) / ptm_B.value) * 100 > g."Property tax State GSDP"
            then 'Yes'
            else 'No'
        end as "Eligible for Property Tax Condition"

    from ulb_years uy

    -- Year T-1
    left join years y_A
        on y_A.year = (
            (substring(uy.design_year from 1 for 4)::integer - 1)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 1)::text
        )

    -- Year T-2
    left join years y_B
        on y_B.year = (
            (substring(uy.design_year from 1 for 4)::integer - 2)::text || '-' ||
            (substring(uy.design_year from 6 for 2)::integer - 2)::text
        )

    -- Property tax values
    left join property_tax_mapper ptm_A
        on uy.ulb_id = ptm_A.ulb and y_A.year_id = ptm_A.year_id
    left join property_tax_mapper ptm_B
        on uy.ulb_id = ptm_B.ulb and y_B.year_id = ptm_B.year_id

    -- States and GSDP
    left join states s
        on uy.state = s.state_id
    left join state_gsdp g
        on s.state_id = g."stateId"
        and uy.design_year_id = g.design_year_id

    -- Property tax submission status
    left join property_tax_submitted pts
        on uy.ulb_id = pts.ulb and uy.design_year_id = pts.design_year
),

dur_submitted as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year_id,
        case
            when ur.ulb is null then 'No'                              -- No DUR record
            when ur."currentFormStatus" in (4, 6) then 'Yes'           -- Submitted DUR form
            else 'No'                                                  -- Record exists, but not eligible
        end as "DUR Submitted"
    from ulb_years uy
    left join {{ source('cityfinance_prod','utilizationreports') }} ur
        on uy.ulb_id = ur.ulb and uy.design_year_id = ur."designYear"
),

dur_expenditure as (
    select
        uy.ulb_id as ulb,
        uy.design_year_id as design_year_id,
        case
            when ds."DUR Submitted" = 'Yes'
                 and ur."grantPosition"->>'expDuringYr' is not null
                 and (ur."grantPosition"->>'expDuringYr')::numeric > 0
            then 'Yes'
            else 'No'
        end as "DUR Greater than 0 Expenditure"
    from ulb_years uy
    left join {{ source('cityfinance_prod','utilizationreports') }} ur
        on uy.ulb_id = ur.ulb and uy.design_year_id = ur."designYear"

    -- Join with DUR submitted status
    left join dur_submitted ds
        on uy.ulb_id = ds.ulb and uy.design_year_id = ds.design_year_id
),

-- Baseline year for baseline submission (2021-22)
baseline_year as (
    select _id as year_id
    from {{ source('cityfinance_prod','years') }}
    where year = '2021-22'
),

baseline_ulbs as (
    -- ULBs that have approved baseline submission for the baseline year
    select distinct
        x.ulb
    from {{ source('cityfinance_prod','xvfcgrantulbforms') }} x
    join baseline_year
        on x.design_year = baseline_year.year_id
    where x.status = 'APPROVED'
)

select
    s.state_name as "State Name",  -- State name for the ULB
    s.state_id as state_id,        -- State ID
    ic.iso_code as "iso_code",     -- ISO code for the state
    uy.ulb_name as "ULB Name",     -- ULB name
    uy.design_year as "Year",      -- Financial year
    uy.design_year_id as design_year_id,  -- Year ID

    -- Annual accounts provisional and audited status
    a."Annual Accounts Provisional",
    a."Annual Accounts Audited",

    -- Both provisional and audited accounts present
    case
        when a."Annual Accounts Provisional" = 'Yes' 
        and a."Annual Accounts Audited" = 'Yes'
        then 'Yes' else 'No'
    end as "Annual Accounts Both Accounts",

    -- State GSDP value for property tax
    g."Property tax State GSDP",  

    -- Property tax submission status
    p."Property tax Submitted",

    -- Property tax growth rate value A=T-1 & B=T-2 and year
    gv.value_A || ' (' || gv.value_A_year || ')' as "Property Tax T-1",
    gv.value_B || ' (' || gv.value_B_year || ')' as "Property Tax T-2",

    -- Property tax growth rate calculation
    case
        when gv.value_B is not null and gv.value_B != 0 then
            round(((gv.value_A - gv.value_B) / gv.value_B) * 100, 2)
        else null
    end as growth_rate_of_ulb,

    -- ✅ Eligibility precomputed in growth_values
    gv."Eligible for Property Tax Condition" as "Eligible in Property Tax Condition",

    -- ✅ Un-Tied Grant Eligibility
    case
        when a."Annual Accounts Provisional" = 'Yes'
         and a."Annual Accounts Audited" = 'Yes'
         and gv."Eligible for Property Tax Condition" = 'Yes'
        then 'Yes' else 'No'
    end as "Condition full fill for Un-Tied Grants",

    -- DUR
    d."DUR Submitted",  
    de."DUR Greater than 0 Expenditure",

    -- Baseline submission status
    case
        when bu.ulb is not null then 'Yes' else 'No'
    end as "Baseline Submission",

    -- ✅ Tied Grant Eligibility
    case
        when a."Annual Accounts Provisional" = 'Yes'
         and a."Annual Accounts Audited" = 'Yes'
         and gv."Eligible for Property Tax Condition" = 'Yes'
         and d."DUR Submitted" = 'Yes'
         and de."DUR Greater than 0 Expenditure" = 'Yes'
         and bu.ulb is not null
        then 'Yes' else 'No'
    end as "Condition full fill for Tied Grants" 

from ulb_years uy
join states s
    on uy.state = s.state_id
left join iso_codes ic
    on s.state_name = ic.state    
left join annual_accounts a
    on uy.ulb_id = a.ulb
    and uy.design_year_id = a.design_year
left join state_gsdp g
    on s.state_id = g."stateId"
   and uy.design_year_id = g.design_year_id
left join property_tax_submitted p
    on uy.ulb_id = p.ulb
    and uy.design_year_id = p.design_year
left join growth_values gv
    on uy.ulb_id = gv.ulb_id
    and uy.design_year = gv.design_year
left join dur_submitted d
    on uy.ulb_id = d.ulb
    and uy.design_year_id = d.design_year_id
left join dur_expenditure de
    on uy.ulb_id = de.ulb
    and uy.design_year_id = de.design_year_id
left join baseline_ulbs bu
    on uy.ulb_id = bu.ulb