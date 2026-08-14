---
name: sql-business-logic
description: Reference for this project's (dbt_cityfinance) recurring business-logic SQL shapes — CAGR calculation, grants_condition fold logic, line-item classification, design-year arithmetic. Use when writing or modifying business logic that resembles these patterns, so new logic follows the established shape instead of reinventing it.
---

# SQL business logic patterns (dbt_cityfinance)

These are the domain-logic shapes that recur across modules. Match them when adding similar logic rather than inventing a new approach — reviewers and downstream consumers expect this shape. Full architectural context: [../../docs/architecture.md](../../docs/architecture.md).

## CAGR (Compound Annual Growth Rate)

Formula, seen ~29 times in `models/afs_analysis/marts/afs_financial_diagnosis.sql` for different metrics:

```sql
ROUND(((POWER(
    end_year_value / NULLIF(start_year_value, 0),
    1.0 / n
) - 1) * 100)::NUMERIC, 2)
```

- Guard the denominator with `NULLIF(..., 0)` — division by zero is expected input, not an edge case to error on.
- `n` is the number of years between start and end (e.g. 3 for 2019-20 → 2022-23).
- The CAGR value is unioned in as a **synthetic pseudo-year row** alongside the real per-year rows: `'CAGR' AS financial_year`, with a sentinel sort value (e.g. `9999 AS financial_year_start`) so it sorts after real years in the final output. Follow this shape (`UNION ALL` a synthetic row, don't add CAGR as an extra column on every row) when adding a new CAGR metric — it matches how the reporting layer expects to consume year-over-year data.

## Line-item / head-of-account classification (cf_municipal_finance_rt)

`cf_municipal_finance_master` explodes a raw JSON `lineItems` payload into one row per line item, then derives:
- `headOfAccount` from the first digit of `majorcode`: `1=Revenue, 2=Expenditure, 3=Liability, 4=Asset, otherwise 'Other'`.
- Downstream, `cf_municipal_finance_revenue_breakdown` further classifies top-level revenue rows into three parallel taxonomies (`revenue` category, `osr` sub-type, `property_tax` flag), each populated only when the row matches that specific classification's rule set, `NULL` otherwise (not a shared enum column). If you add a new classification dimension, follow this "one nullable column per taxonomy, populated only for matching rows" shape rather than cramming multiple taxonomies into one column.
- `cf_municipal_finance_master` is the single foundational model other marts in the module build on (`cf_municipal_finance_major_heads` rolls it up to major-code level; `cf_municipal_finance_validations` runs 22 data-quality rules over it). Changes to its classification logic ripple to all three — check all three schema.yml descriptions in `models/cf_municipal_finance_rt/schema.yml` before changing `cf_municipal_finance_master`.

## grants_condition "fold" models

`grants_condition/marts/` has 7 models named `fold1Summary`, `fold1aUAs`, `fold1aUntiedAndTied`, `fold1bUAs`, `fold1bUntiedAndTied`, `fold2aUAs`, `fold2bUntiedAndTied` — these represent staged eligibility/condition-checking logic across grant assessment periods ("folds"), where `fold1b`/`fold2a` models join a design year against **prior** years via `{{ design_year_minus('uy.design_year', n) }}` (n=1 or 2) to evaluate multi-year conditions. When adding a new fold-style model, check the sibling fold model closest in naming (e.g. a new `fold2b` variant should mirror `fold2aUAs`'s year-lookback structure) rather than designing from scratch.

## Design-year arithmetic

Always use `{{ design_year_minus('col', n) }}` for `'YYYY-YY'` year subtraction — never hand-roll substring/int arithmetic inline. The macro will error (not silently produce garbage) on malformed input, which is the intended behavior; don't wrap it in a try/fallback.

## Fuzzy ULB matching (afs_analysis)

`afs_financial_diagnosis.sql` matches ULB records across tables that don't share a clean id (e.g. bond issuer data joined against a ULB master list) using the `ulb_join_key` normalization (see [sql-review](../sql-review/SKILL.md)). This is currently the only place this pattern is applied — if a new module needs to join on ULB name rather than id, this is the established convention to reuse, not a one-off.

## Dynamic column matching (form-derived data)

`nmam_ulb_response.sql`, `afs_ocr_xl_files_dump.sql`, and `marketreadiness_summary.sql` all deal with source tables whose columns are named after externally-controlled labels (Google Form question text, OCR'd spreadsheet headers) that don't have stable identifiers. They use `adapter.get_columns_in_relation(source(...))` at compile time plus `raise_compiler_error` to fail loudly when an expected column can't be matched, rather than silently returning NULL. If you're integrating another form/spreadsheet-derived source, read `nmam_ulb_response.sql` closely first — this is a nontrivial Jinja pattern and the failure-mode behavior (hard compile error vs. silent NULL) is a deliberate choice worth preserving.
