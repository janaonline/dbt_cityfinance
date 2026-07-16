# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is a dbt (dbt-core 1.11, dbt-postgres) project for [CityFinance](https://cityfinance.in/), a Janaagraha initiative. It transforms raw operational data — originally captured in a MongoDB-backed app and replicated into a single shared Postgres database (`cityfinance`) — into per-module reporting/analysis tables (property tax, grants, AFS financial diagnosis, market readiness, etc.) consumed downstream for reporting/export.

## Setup & common commands

```bash
source venv/bin/activate      # local venv already has dbt-core + dbt-postgres
dbt deps                      # install packages from packages.yml — run before any dbt run
dbt debug                     # verify profiles.yml connection
dbt run --target dev          # profiles.yml only defines a `dev` target locally
dbt test --target dev
dbt seed --target dev         # loads seeds/iso_codes.csv
dbt build --target dev        # run + test in DAG order
```

Run a single model:
```bash
dbt run --select stg_growth_rate --target dev
dbt run --select fold1bUntiedAndTied --target dev
```

Run a whole module — prefer `tag:` selection since every module's `marts` block sets `+tags`:
```bash
dbt run --select tag:grants_condition --target dev
dbt run --select tag:property_tax_poc --target dev
```
Folder-path selection also works, but `--select tag:staging` matches staging models across *every* module, not just one — scope it: `dbt run --select models/grants_condition/staging tag:staging`.

`profiles.yml` is git-ignored and holds real dev DB credentials — never commit it. Production runs happen via Dalgo (Prefect-based orchestration), which supplies its own internal `profiles.yml` and always runs `dbt deps` first; don't assume a `profiles.yml` exists when reasoning about prod.

## Architecture

Models are organized **one folder per business module** under `models/`, not by dbt's usual staging/marts-at-the-top layout:

```
models/<module>/
  source.yml       # source declarations (raw tables replicated from MongoDB)
  schema.yml        # (optional) column tests for staging models
  staging/          # (optional) thin cleanup/rename models
  marts/            # final, table-materialized models with business logic
```

Current modules: `property_tax_poc`, `grants_condition`, `grants_allocation`, `ap_api_poc`, `afs_digitisation_tracker`, `afs_analysis`, `market_readiness`, `nmam_ulb_response`. Most modules only have `marts/`; `property_tax_poc` is the one with a full `staging/` layer.

**Schema routing is entirely config-driven, not folder-driven.** Every module needs its own `models: Janaagraha: <module>: { staging: {...}, marts: {...} }` block in `dbt_project.yml` setting `+schema`, `+tags`, and `+materialized` (almost everything here is `table`). When adding a new module, add its block there — without it, models fall back to whatever schema `profiles.yml`/the calling platform provides.

`macros/generate_schema_name.sql` overrides dbt's default schema-name macro to return `+schema` **verbatim** (no `<target>_<custom>` concatenation). This means `+schema` must always be set explicitly per module block — omitting it silently falls back to `target.schema`.

## Data model conventions

- Sources (`source.yml` in each module) point at Postgres schemas holding data replicated from the MongoDB app — commonly `cf_prod`, `mongo_staging`, `cf_staging`. Raw tables use Mongo conventions: `_id` as primary key, booleans stored as the strings `'true'`/`'false'` (e.g. `WHERE u."isActive" = 'true'`), and mixed-case quoted column names (`"isPublish"`, `"ulbType"`).
- Two shared macros used throughout marts models — always pass the column as a **quoted string**, not a bare ref:
  - `{{ safe_numeric('ptm.value') }}` (`macros/safe_numeric.sql`) — cleans/validates messy numeric strings (commas, whitespace, leading/trailing dots) and casts to `numeric`, returning `NULL` if invalid.
  - `{{ design_year_minus('uy.design_year', 1) }}` (`macros/design_year_minus.sql`) — subtracts `n` years from a `'YYYY-YY'` design-year string.
- When a clean `_id` join between a source table and a JSON/derived table isn't available, models normalize ULB names into a fuzzy join key: `LOWER(REGEXP_REPLACE(BTRIM(col::TEXT), '[[:space:]]+', ' ', 'g'))`, typically aliased `ulb_join_key`.
- Financial line items are often stored as JSON blobs (e.g. `lineitems_json ->> '110'`); values are extracted with a numeric-regex guard (`~ '^-?[0-9]+(\.[0-9]+)?$'`) before casting, since JSON keys are free-form strings.
- CAGR columns follow a repeated pattern: compute `POWER(end_year_value / start_year_value, 1.0/n) - 1) * 100`, guarding division by zero with `NULLIF`, and union the result as a synthetic `'CAGR'` pseudo-year row alongside the per-year rows.
- Marts models are typically single large `SELECT`s built from CTEs, with final output columns aliased as human-readable, quoted "Title Case" strings (e.g. `"Total ULBs"`, `"Property Tax as % of OSR"`) since these tables are consumed directly for reporting/export rather than by further models.

## Packages (`packages.yml`)

`dbt_utils` 1.3.0, `dbt_expectations` 0.10.4, `elementary-data/elementary` 0.16.2. Elementary gets its own schema (`models: elementary: +schema: "elementary"` in `dbt_project.yml`), and the project-level flag `require_explicit_package_overrides_for_builtin_materializations: false` exists specifically for Elementary/dbt 1.8+ compatibility — don't remove it.
