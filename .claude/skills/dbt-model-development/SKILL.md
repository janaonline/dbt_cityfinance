---
name: dbt-model-development
description: Add or modify a single model within an existing module of this dbt project (dbt_cityfinance). Use when the task is "add a model", "add a column", "fix a mart", or similar scoped, single-model work — not for creating a brand-new module (see dbt-module-development).
---

# dbt model development (dbt_cityfinance)

Practical steps for adding/editing one model inside an existing module. See [../../docs/architecture.md](../../docs/architecture.md) and [../../docs/folder_structure.md](../../docs/folder_structure.md) for full background.

## Where the file goes

- Almost every module only has `marts/` — put the new model there: `models/<module>/marts/<name>.sql`.
- `property_tax_poc` is the **only** module with a `staging/` folder. If you're adding a thin cleanup/rename/id-resolution model in `property_tax_poc`, it goes in `staging/`; if it's the final business-logic output, it goes in `marts/`. Don't add a `staging/` folder to any other module without first checking whether that's actually intended (see dbt-module-development).

## Required config() block

Every SQL file in this project opens with its own `{{ config(...) }}` block — even though `dbt_project.yml` already sets schema/tags/materialized at the folder level. Match the existing per-module style, e.g.:

```sql
{{ config(
    materialized='table',
    tags=['<module>']
) }}
```

Look at a sibling file in the same module folder and copy its exact `config()` shape (tag list, single-line vs multi-line) rather than inventing a new style. All materializations in this project are `table` — don't introduce `view`/`incremental`/`ephemeral` without discussing it first, since nothing here currently uses them.

## Schema/tags come from dbt_project.yml, not the file

The actual output schema for your new model is whatever `+schema` is set in the module's block in `dbt_project.yml` — **not** derived from the folder path. Before assuming where a model will land, check the module's block there. Note two live gaps: `property_tax_poc`'s `+tags` lines are commented out (so `--select tag:property_tax_poc` won't currently select anything), and `grants_condition`'s `staging:` sub-block is fully commented out. Don't "fix" these as a side effect of an unrelated model change — call it out to the user if it's blocking you.

When you run your new model locally with `--target dev` — or with no `--target` flag at all, since `dev` is `profiles.yml`'s default — it lands in the shared `dev_schema`, not the module's configured `+schema` (see [dbt-debugging](../dbt-debugging/SKILL.md)). To deliberately verify it lands in the real schema `dbt_project.yml` configures, pass `--target prod` explicitly.

## Reuse the existing macros — don't reimplement their logic inline

- `{{ safe_numeric('table.col') }}` — always pass the column as a **quoted string**. Use this instead of a bare `::numeric` cast whenever a value could contain commas, whitespace, or malformed input (i.e. almost any Mongo-sourced numeric-as-text field).
- `{{ design_year_minus('table.design_year_col', n) }}` — use this instead of hand-rolling `'YYYY-YY'` arithmetic when joining a row to a prior design year. Requires strict `'YYYY-YY'` input format.

Both macros are defined in `macros/` with full usage docs in their own header comments — read the macro file if the usage isn't obvious from an existing call site.

## Follow the module's existing SQL conventions

Look at 1-2 sibling models in the same module before writing new SQL, and match:
- Whether raw sources are joined by `_id` or by the fuzzy `ulb_join_key` pattern (`LOWER(REGEXP_REPLACE(BTRIM(col::TEXT), '[[:space:]]+', ' ', 'g'))`).
- Whether final output columns are aliased as quoted Title Case (`"Total ULBs"` — reporting/export modules: `afs_analysis`, `nmam_ulb_response`, `grants_condition`, `property_tax_poc`) or plain/camelCase (`cf_municipal_finance_rt`, `ap_api_poc` — dashboard/API-feeding modules in the shared `cf_requests` schema).
- Booleans are stored as the strings `'true'`/`'false'` in raw Mongo-replicated tables — compare with `= 'true'`, not `IS TRUE`.

See the [sql-review](../sql-review/SKILL.md) and [sql-business-logic](../sql-business-logic/SKILL.md) skills for the full pattern checklist.

## Running and verifying

```bash
source venv/bin/activate
dbt run --select <model_name> --target dev
```

To also pull in what it depends on: `dbt run --select +<model_name> --target dev`. If the module has a `schema.yml` (`property_tax_poc`, `cf_municipal_finance_rt`), run its tests too: `dbt test --select <model_name> --target dev`. `profiles.yml` (gitignored, local) must already point at a valid dev DB — this repo does not create one for you.

## After the change

Check [dbt-change-review](../dbt-change-review/SKILL.md) before considering the task done, and [dbt-documentation-maintenance](../dbt-documentation-maintenance/SKILL.md) to see whether this change actually needs a docs update (most single-model additions don't).
