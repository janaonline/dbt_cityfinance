---
name: sql-review
description: Review dbt/SQL model changes in this project (dbt_cityfinance) for correctness against its actual conventions — Mongo-sourced data quirks, macro usage, join patterns, output style. Use before finishing any model edit, or when asked to review a diff/PR touching models/.
---

# SQL review checklist (dbt_cityfinance)

Repo-specific checks — not generic SQL style advice. Cross-check against [../../docs/architecture.md](../../docs/architecture.md) for the full pattern reference.

## Mongo-sourced data conventions

- [ ] Boolean-looking columns from raw source tables are compared as strings: `WHERE u."isActive" = 'true'`, never `IS TRUE`/`IS FALSE` or a bare boolean cast — these fields are stored as the strings `'true'`/`'false'`.
- [ ] Mixed-case column names from raw tables are quoted exactly as declared (`"isPublish"`, `"ulbType"`, `"currentFormStatus"`) — an unquoted reference will fold to lowercase and fail to match.
- [ ] `_id` is used as the join key to raw source tables where a clean id relationship exists — don't introduce a name-based join where an id-based one is available.

## Numeric handling

- [ ] Any numeric value pulled from a messy text/JSON source field goes through `{{ safe_numeric('col') }}` rather than a bare `::numeric` cast. A raw cast will hard-error on a single bad row; `safe_numeric` returns `NULL` instead.
- [ ] `safe_numeric` is called with the column as a **quoted string** (`safe_numeric('ptm.value')`), not a bare Jinja expression (`safe_numeric(ptm.value)`) — the latter is silently wrong (tries to resolve a Jinja var).
- [ ] Be aware `safe_numeric`'s internal regex is looser than its own header comment describes (unescaped `.` instead of `\.` for the decimal point) — see architecture.md. Don't "fix" this as a drive-by change in an unrelated PR; flag it if it's actually causing a bug.

## JSON line-item extraction

- [ ] Any `->>'code'` / `->>'key'` extraction from a JSON/JSONB line-items blob is guarded with a numeric regex (`~ '^-?[0-9]+(\.[0-9]+)?$'`) before being cast, since JSON keys/values here are free-form and not schema-validated.
- [ ] If the source column is stored as `TEXT` rather than native `jsonb`, it's explicitly cast (`NULLIF(col::TEXT,'')::jsonb`) before the `->>` — don't assume a text column is already JSON-typed.
- [ ] When exploding a JSONB array into scalar rows (not extracting an object key), `jsonb_array_elements_text(...)` is used, not `jsonb_array_elements(...)` + `CAST(...AS text)`/`::text` — the latter leaves JSON string quoting in place and breaks comparisons against plain text/numeric columns.

## Joins

- [ ] Where no clean `_id` relationship exists between a source table and a JSON/derived table, the fuzzy `ulb_join_key` pattern is used consistently on both sides of the join: `LOWER(REGEXP_REPLACE(BTRIM(col::TEXT), '[[:space:]]+', ' ', 'g'))`. Check both the left and right side use the identical normalization — a mismatch (e.g. one side missing `BTRIM`) silently drops matches instead of erroring.
- [ ] Design-year joins use `{{ design_year_minus(...) }}` rather than hand-written substring arithmetic.

## Output conventions

- [ ] Final column aliasing matches the module's existing consumer: reporting/export modules (`afs_analysis`, `nmam_ulb_response`, `grants_condition`, `property_tax_poc`) use quoted Title Case (`"Total Own Source Revenue"`); dashboard/API-feeding modules in the shared `cf_requests` schema (`cf_municipal_finance_rt`, `ap_api_poc`, `afs_digitisation_tracker`, `market_readiness`) use plain/camelCase. Don't introduce Title Case aliases into a `cf_requests` module or vice versa without confirming the consumer expects the change.
- [ ] `config()` block at the top of the file matches sibling models in the same module (tag list, `materialized='table'`) — don't invent a new materialization.

## Testing/data-quality

- [ ] If the module has a `schema.yml` (`property_tax_poc`, `cf_municipal_finance_rt`), check whether the changed/added column should get a `not_null`/`accepted_values` test, following the existing style (including using `# TODO` comments for deliberately deferred tests rather than silently skipping).
- [ ] Don't add `dbt_expectations.*` tests speculatively — it's an installed-but-currently-unused package; only reach for it if a genuinely new kind of check is needed that generic/dbt_utils tests can't express.

## Scope discipline

- [ ] Don't reformat/restyle unrelated parts of a mart file (these are large single-`SELECT`-built-from-CTEs files, often hundreds of lines) as a side effect of a small fix.
- [ ] Don't touch `dbt_project.yml` schema/tag config unless the task specifically requires a routing change — see [dbt-module-development](../dbt-module-development/SKILL.md) if it does.
