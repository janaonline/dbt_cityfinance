---
name: dbt-module-development
description: Stand up a brand-new business module (a new top-level folder under models/) in this dbt project (dbt_cityfinance). Use when the task is "add a new module", "onboard a new data source/reporting area", or similar — not for adding one model to an existing module (see dbt-model-development).
---

# dbt module development (dbt_cityfinance)

This project organizes models one folder per business module under `models/`, not dbt's usual staging/marts-at-the-top layout. Nine modules exist today: `property_tax_poc`, `grants_condition`, `grants_allocation`, `ap_api_poc`, `afs_digitisation_tracker`, `afs_analysis`, `market_readiness`, `nmam_ulb_response`, `cf_municipal_finance_rt`. See [../../docs/folder_structure.md](../../docs/folder_structure.md) for the full inventory of what each currently contains.

## Steps to create a new module `<module>`

1. **Create the folder and `source.yml`**: `models/<module>/source.yml`. Every existing module has one — declare the Postgres schema (commonly `cf_prod`, `mongo_staging`, or `cf_staging`) and the raw tables the module needs. Check whether the tables you need are already declared as a source somewhere else in the project first (sources are shared/cross-referenced across modules — e.g. `states`/`ulbs`/`ulbtypes` are declared once in `grants_condition/source.yml` and reused by other modules' marts) before re-declaring them under a new source name.
2. **Decide staging vs. marts-only**: 8 of 9 existing modules skip `staging/` entirely and go straight from `source()` to `marts/`. Only add `staging/` if you have genuine thin cleanup/rename/id-resolution work to do before the main business logic — look at `models/property_tax_poc/staging/stg_tax_sub_rate.sql` as the template for that shape (CTEs resolving foreign-key ids to human-readable names).
3. **Add a `dbt_project.yml` block.** This is the step that's easy to forget and silently breaks schema output. Under `models: Janaagraha:`, add:
   ```yaml
   <module>: # to run the <module> models : dbt run --select tag:<module>
     marts:
       +schema: <module>_prod        # or the shared 'cf_requests' schema if this module feeds a dashboard/API rather than reporting export
       +tags: ['<module>']
       +materialized: table
   ```
   **`+schema` must be set explicitly.** `macros/generate_schema_name.sql` returns whatever `+schema` says verbatim (no `<target>_<custom>` prefixing) for any non-`dev` target — omit it and the model falls back to `target.schema` instead. Match the existing 6-space indentation style used by every module block except `cf_municipal_finance_rt` (which has a known indentation inconsistency — don't copy that one).

   Note: under `--target dev` — or no `--target` flag at all, since `dev` is `profiles.yml`'s default — the macro overrides `+schema` entirely and always resolves to the shared `dev_schema` (see [dbt-debugging](../dbt-debugging/SKILL.md)) — this is intentional and applies to your new module automatically, no extra config needed. Your module's `+schema` only takes effect when you explicitly pass `--target prod`.
4. **Decide the schema name**: if this module feeds the reporting/export path, give it its own `<module>_prod` schema (like `property_tax_poc_prod`, `grants_condition_prod`, `grants_allocation_prod`). If it feeds a dashboard/API directly, consider whether it belongs in the shared `cf_requests` schema alongside `ap_api_poc`, `afs_digitisation_tracker`, `market_readiness`, `afs_analysis`, `nmam_ulb_response`, `cf_municipal_finance_rt` — check with whoever owns the consuming dashboard/API before deciding.
5. **Write the marts model(s)**, following [dbt-model-development](../dbt-model-development/SKILL.md) and the SQL conventions in [sql-review](../sql-review/SKILL.md)/[sql-business-logic](../sql-business-logic/SKILL.md).
6. **Add `schema.yml` tests if warranted** — most modules don't have one; only `property_tax_poc` and `cf_municipal_finance_rt` do. See [dbt-testing](../dbt-testing/SKILL.md) for the pattern to follow if you add one.

## Verifying the new module

```bash
dbt run --select tag:<module> --target dev
dbt test --select tag:<module> --target dev   # only meaningful if you added a schema.yml
```

Confirm the model landed in the schema you expect by checking the output of `dbt run` (it prints the target relation) against your `dbt_project.yml` block.

## After adding a module

New modules are exactly the case flagged in the doc-maintenance rule — review [../../docs/folder_structure.md](../../docs/folder_structure.md), [../../docs/architecture.md](../../docs/architecture.md) (module inventory table and schema-routing table), and the root `CLAUDE.md` module list, and update them. See [dbt-documentation-maintenance](../dbt-documentation-maintenance/SKILL.md).
