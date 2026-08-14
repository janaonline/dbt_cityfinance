# Architecture

How data flows through this project, how schema/materialization/tags are decided, the SQL patterns used repeatedly across modules, and the testing setup. See [folder_structure.md](folder_structure.md) for where files physically live.

## Data flow

```
Postgres (cityfinance DB, replicated from MongoDB app)
  └─ source tables (cf_prod / mongo_staging / cf_staging / grants_allocation_prod schemas)
        │
        │  declared per-module in models/<module>/source.yml
        ▼
  staging models   (only models/property_tax_poc/staging/ — thin cleanup/rename/join-to-names)
        │
        ▼
  marts models     (every module — final business logic, single large SELECT built from CTEs)
        │
        ▼
  reporting/export schemas (property_tax_poc_prod, grants_condition_prod, grants_allocation_prod,
                             cf_requests — see routing table below)
        │
        ▼
  consumed downstream for reporting/export (dashboards, APIs) outside this repo
```

Only `property_tax_poc` interposes a staging layer between source and marts. Every other module's marts models select directly from `source(...)`.

**Sources are not strictly private to a module.** Several modules' marts reference `source('cityfinance_prod', ...)` for tables like `states`, `ulbs`, `ulbtypes` even though `cityfinance_prod` is declared in `models/grants_condition/source.yml`, not their own. When tracing a model's lineage, check `grants_condition/source.yml` for `cityfinance_prod` table declarations even if you're working in a different module.

## Schema routing

Schema, tags, and materialization are controlled entirely by the `models: Janaagraha: <module>: { staging: {...}, marts: {...} }` blocks in `dbt_project.yml` — **not** by folder location. `macros/generate_schema_name.sql` overrides dbt's default schema macro to return `+schema` **verbatim**, with no `<target>_<custom>` concatenation dbt normally applies. This means:

- Every module block must set `+schema` explicitly — omitting it falls back to `target.schema`.
- The schema a model lands in has no relationship to its folder path — check `dbt_project.yml`, not `models/<module>/`, to find out where a model actually materializes.

**Dev-target override.** `generate_schema_name.sql` special-cases the literal target name `dev`: when `target.name == 'dev'`, it always returns `target.schema` (`dev_schema`), ignoring `+schema` entirely — every module lands together in one shared dev schema, regardless of its configured production schema. This is safe because dbt requires globally-unique model names across the whole project, so there's no collision risk between modules sharing one dev schema. **Any other target** falls through to the original verbatim-`+schema` behavior, unchanged — the override only ever triggers on the literal target name `dev`.

This matters locally because `profiles.yml` (git-ignored) defines two targets against the same shared Postgres instance/database used by everything else (Dalgo's production orchestration is entirely separate, with its own internal `profiles.yml`/target, invisible to this repo):
- `dev` — schema `dev_schema`; **the profile's default**, used both when `--target` is omitted and when passed explicitly (`--target dev`).
- `prod` — must be requested explicitly via `--target prod`; resolves through each module's real `+schema` (`property_tax_poc_prod`, `cf_requests`, etc.), i.e. the same schemas Dalgo's production runs write to.

This keeps the usual "safe by default" convention: a bare `dbt run`/`dbt build` (no `--target` flag) lands in the sandbox `dev_schema`, same as explicit `--target dev`. Only a deliberate `--target prod` reaches the real production-named schemas on the shared database. Any other developer maintaining their own local `profiles.yml` should replicate this same `dev`/`prod`-named-target convention (target names matter — the macro only special-cases the string `dev`) for `generate_schema_name.sql` to behave consistently for them.

The routing table below reflects the schema each module resolves to **for any non-`dev` target** (i.e. this repo's local `prod` target, or Dalgo's production orchestration). Under `--target dev`, every row instead resolves to `dev_schema`.

| Module | staging schema | marts schema | tags | materialized |
|---|---|---|---|---|
| `property_tax_poc` | `property_tax_poc_staging` | `property_tax_poc_prod` | *(commented out — currently no tags apply)* | table |
| `grants_condition` | *(staging block commented out, unused)* | `grants_condition_prod` | `grants_condition`, `marts` | table |
| `grants_allocation` | — | `grants_allocation_prod` | `grants_allocation`, `marts` | table |
| `ap_api_poc` | — | `cf_requests` | `ap_api_poc` | table |
| `afs_digitisation_tracker` | — | `cf_requests` | `afs_digitisation_tracker` | table |
| `market_readiness` | — | `cf_requests` | `market_readiness` | table |
| `afs_analysis` | — | `cf_requests` | `afs_analysis` | table |
| `nmam_ulb_response` | — | `cf_requests` | `nmam_ulb_response` | table |
| `cf_municipal_finance_rt` | — | `cf_requests` | `cf_municipal_finance_rt` | table |
| `elementary` (package) | — | `elementary` | — | package default |
| seeds (all) | — | `cf_prod` | — | — |

Known quirks worth knowing about (documented, not "fixed", since fixing them isn't required by any current task):
- `property_tax_poc`'s `+tags` lines are commented out in both its `staging` and `marts` blocks — `dbt run --select tag:property_tax_poc` (as documented in the file's own inline comment) currently won't select anything by tag; use folder-path selection (`--select models/property_tax_poc`) instead, or path+tag combos as noted in the root `CLAUDE.md`.
- `grants_condition`'s `staging:` sub-block is entirely commented out (module has no staging models at all despite the placeholder existing).
- The `cf_municipal_finance_rt` block's `marts:` key is indented at 8 spaces instead of the 6 spaces every other module uses. YAML still parses it correctly (it's a mapping key, not a list), but it's visually inconsistent — worth matching the 6-space style if that block is ever touched.
- Six modules (`ap_api_poc`, `afs_digitisation_tracker`, `market_readiness`, `afs_analysis`, `nmam_ulb_response`, `cf_municipal_finance_rt`) all share the single `cf_requests` schema — these are the modules whose marts feed dashboards/APIs directly (see "Output style" below) rather than reporting exports.

## Materialization

Every model in the project — staging and marts alike — is materialized as `table`. There is no `view`, `incremental`, or `ephemeral` model anywhere. Every SQL file also sets its own `{{ config(materialized='table', tags=[...]) }}` block at the top, layered on top of (and consistent with) the `dbt_project.yml` block. One model overrides its alias: `models/nmam_ulb_response/marts/nmam_ulb_response.sql` sets `alias = 'nmam_ulb_response'` explicitly (functionally a no-op here since it matches the filename, but present as an explicit override).

## Macros

Three project macros exist in `macros/`, all documented inline with usage examples in their own headers:

- **`generate_schema_name(custom_schema_name, node)`** — see "Schema routing" above.
- **`safe_numeric(col)`** — cleans a string expression (trims whitespace, strips commas/spaces) and validates it against a numeric regex before casting to `numeric`; returns `NULL` on anything that doesn't validate. Always call with the column as a **quoted string**: `{{ safe_numeric('ptm.value') }}`, not `{{ safe_numeric(ptm.value) }}` (the latter tries to resolve `ptm.value` as a Jinja variable). Used throughout `afs_analysis`, `grants_condition`, and `cf_municipal_finance_rt` marts, e.g. `models/cf_municipal_finance_rt/marts/cf_municipal_finance_master.sql:26`, `models/afs_analysis/marts/afs_financial_diagnosis.sql:77`.
  - **Known discrepancy:** the macro's header comment documents/proposes a regex using escaped `\.` for the decimal point (`'^[-+]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)$'`), but the actual regex used in the macro body is `'^([-+]?(\d+(.\d*)?|.\d+))$'` — an *unescaped* `.` that matches any character, not just a literal decimal point. In practice this is rarely triggered because inputs go through `regexp_replace(...,'[, ]','','g')` first, but it means a value like `"12a34"` would pass validation where the documented/intended regex would reject it. This is a real, pre-existing gap between the comment and the code — noted here for awareness, not changed as part of documentation work.
- **`design_year_minus(design_year_col, n)`** — subtracts `n` years from a `'YYYY-YY'`-format design-year string (e.g. `'2022-23'` minus 1 → `'2021-22'`). Requires the input to already be in that exact format; will error on malformed input rather than returning NULL. Used only in `grants_condition` marts (`fold1bUAs.sql`, `fold1bUntiedAndTied.sql`, `fold2aUAs.sql`, `fold2bUntiedAndTied.sql`) to join a model's design year against the *prior* year's/years' rows.

## Recurring SQL patterns

These patterns show up repeatedly across marts models. See [sql-business-logic skill](../.claude/skills/sql-business-logic/SKILL.md) and [sql-review skill](../.claude/skills/sql-review/SKILL.md) for how to apply/check them when writing or reviewing SQL.

- **Mongo-sourced raw data conventions**: `_id` as primary key, booleans stored as the *strings* `'true'`/`'false'` (not real booleans — always compare with `= 'true'`, never `IS TRUE`), and mixed-case quoted column names (`"isPublish"`, `"ulbType"`, `"currentFormStatus"`).
- **JSON line-item extraction**: financial line items are stored as JSON/JSONB blobs keyed by free-form code strings, e.g. `lineitems_json ->> '110'` or `NULLIF("lineItems"::TEXT,'')::jsonb ->> '240'`. Because JSON keys are arbitrary and the values are strings, extractions are always guarded with a numeric regex (`~ '^-?[0-9]+(\.[0-9]+)?$'`) before casting — see `models/afs_analysis/marts/afs_revenue_resources.sql:233-234`, `models/market_readiness/marts/marketreadiness_summary.sql:161`.
- **Fuzzy ULB join key**: when a clean `_id` join isn't available between a source table and a JSON/derived table, names are normalized with `LOWER(REGEXP_REPLACE(BTRIM(col::TEXT), '[[:space:]]+', ' ', 'g'))`, aliased `ulb_join_key`. Currently this exact pattern appears only in `models/afs_analysis/marts/afs_financial_diagnosis.sql` (lines 129, 889) — treat it as the established convention to reuse for any new fuzzy ULB-name join, not (yet) something already applied project-wide.
- **CAGR pattern**: `POWER(end_year_value / NULLIF(start_year_value, 0), 1.0/n) - 1) * 100`, rounded, with a synthetic `'CAGR'` row `UNION ALL`'d alongside the normal per-year rows (see `models/afs_analysis/marts/afs_financial_diagnosis.sql:107-124`, repeated ~29 times in that file for different metrics). The synthetic row typically carries a sentinel sort value (e.g. `9999 AS financial_year_start`) so it sorts after real years.
- **Dynamic column introspection**: a few models use `{%- set columns = adapter.get_columns_in_relation(source(...)) -%}` at compile time to fuzzy-match dynamically-named source columns (e.g. Google-Form-exported headers) against expected fields, raising `raise_compiler_error` on missing/ambiguous matches. Seen in `models/nmam_ulb_response/marts/nmam_ulb_response.sql`, `models/afs_digitisation_tracker/marts/afs_ocr_xl_files_dump.sql`, `models/market_readiness/marts/marketreadiness_summary.sql`. This is the most advanced Jinja usage in the project — read one of these files closely before extending that pattern.
- **Output column style depends on the consumer**: modules feeding the reporting/export path (`afs_analysis`, `nmam_ulb_response`, `grants_condition`, `property_tax_poc`) alias most final columns as quoted, human-readable "Title Case" strings (e.g. `"Total Own Source Revenue"`, `"Tax Revenue as % of OSR"`). Modules feeding dashboards/APIs directly (`cf_municipal_finance_rt`, `ap_api_poc` — the `cf_requests`-schema modules) instead keep camelCase for the one field that needs quoting (`"headOfAccount"`) and otherwise use plain lowercase columns. Match the existing convention of whichever module you're editing rather than defaulting to Title Case everywhere.
- One Postgres-specific optimizer hint appears once: `models/cf_municipal_finance_rt/marts/cf_municipal_finance_master.sql:24` uses `lineitemslegends AS MATERIALIZED (...)` inside a CTE — a query planner hint, unrelated to dbt's own `materialized` config.

## Testing architecture

Only generic (built-in + `dbt_utils`) tests are used, declared in `schema.yml`. Only 2 of 9 modules have a `schema.yml` at all:

- **`models/property_tax_poc/schema.yml`** — tests one model, `stg_tax_sub_rate`: `not_null` on `ulb`, `year`, `state`, `status`.
- **`models/cf_municipal_finance_rt/schema.yml`** — tests four marts models (`cf_municipal_finance_master`, `cf_municipal_finance_major_heads`, `cf_municipal_finance_revenue_breakdown`, `cf_municipal_finance_validations`) using `not_null`, `accepted_values`, and one model-level `dbt_utils.unique_combination_of_columns` (on `ulb, state, year` for `cf_municipal_finance_validations`). Several columns carry `# TODO` comments explaining a deliberately *deferred* test (e.g. "add not_null once legend match-rate is verified") rather than an oversight — read these before assuming a missing test is a gap to fill.

`dbt_expectations` 0.10.4 is declared in `packages.yml` and installed, but **no `dbt_expectations.*` test is used anywhere in the project** (confirmed via `grep -rl dbt_expectations models/` returning nothing outside `package-lock.yml`). It's available if a future test needs it (e.g. distribution/range checks) but is currently unused.

The other 7 modules have no declared tests at all. `analyses/`, `snapshots/`, and `tests/` (dbt's custom-singular-test directory) all exist as directories (per `dbt_project.yml`'s `analysis-paths`/`snapshot-paths`/`test-paths`) but contain only a `.gitkeep` placeholder — no custom SQL tests, snapshots, or analyses exist in this project today.

## Packages

`dbt_utils` 1.3.0 (used for `dbt_utils.unique_combination_of_columns`), `dbt_expectations` 0.10.4 (installed, unused — see above), `elementary-data/elementary` 0.16.2 (gets its own `elementary` schema; needs `require_explicit_package_overrides_for_builtin_materializations: false` set in `flags:` for dbt 1.8+ compatibility — do not remove this flag). `calogica/dbt_date` 0.10.1 is a resolved transitive dependency of `dbt_expectations` (visible in `package-lock.yml`, not declared directly in `packages.yml`).

## Module architecture summary

Every module is fully self-contained business logic with no cross-module `ref()`s observed (models within a module reference each other and shared sources, but modules don't depend on other modules' marts). This means modules can be run/tested independently via `dbt run --select tag:<module>` (where tags are configured — see the routing table's caveats above) without needing to run the whole project's DAG.
