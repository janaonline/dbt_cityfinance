---
name: dbt-change-review
description: Final checklist before considering any dbt/SQL change in this project (dbt_cityfinance) done — schema/tag consistency, pattern reuse, scope discipline. Use at the end of any model/module change, before reporting the task complete.
---

# Change review checklist (dbt_cityfinance)

Run through this before calling a dbt change finished. This is a pre-completion checklist, distinct from [sql-review](../sql-review/SKILL.md) (which is about SQL correctness patterns) — this one is about whether the change is *complete and consistent* with the rest of the project.

## Config consistency

- [ ] If a new model was added, does its module's `dbt_project.yml` block already cover it (schema/tags/materialized), or does the block need updating (new module) — see [dbt-module-development](../dbt-module-development/SKILL.md)?
- [ ] Does the model's own `{{ config(...) }}` block match its module's other models (same tag list shape, `materialized='table'`)?
- [ ] If you touched `dbt_project.yml`, did you avoid changing any *other* module's config as a side effect? Each module's block is independent — a shared-schema module (`cf_requests`) change affects 6 modules at once.

## Pattern reuse

- [ ] Did the change reuse `{{ safe_numeric(...) }}` / `{{ design_year_minus(...) }}` instead of reimplementing the same cleanup/arithmetic inline?
- [ ] Does new business logic (CAGR, classification, fold-style multi-year joins) follow the shape documented in [sql-business-logic](../sql-business-logic/SKILL.md) rather than a new one-off approach?
- [ ] Does the output column naming style match the module's existing convention (Title Case for reporting/export modules vs. camelCase for `cf_requests`-schema dashboard/API modules)?

## Shared source impact

- [ ] If the change touches a shared source (e.g. `cityfinance_prod`'s `states`/`ulbs`/`ulbtypes`, declared in `grants_condition/source.yml` but referenced from other modules' marts), did you check what else references that source before assuming the change is isolated to one module?

## Testing

- [ ] If the module has a `schema.yml`, does a changed/added column need a test, following the existing not_null/accepted_values style (see [dbt-testing](../dbt-testing/SKILL.md))?
- [ ] Did you actually run `dbt run --select <model> --target dev` (and `dbt test` if applicable) rather than only reading the SQL?

## Scope discipline

- [ ] Did the change stay scoped to what was asked — no unrelated reformatting of large mart files, no speculative test additions, no "while I'm here" schema/tag changes?
- [ ] No credentials or connection details were added to any file (`profiles.yml` is the only place for those, and it's gitignored — never inline a connection string or password in a model, macro, or `.claude/` config file).

## Documentation

- [ ] Does this change actually affect folder structure, architecture, schema routing, a macro, a SQL convention, or test strategy in a way that makes existing docs/skills wrong? If yes, see [dbt-documentation-maintenance](../dbt-documentation-maintenance/SKILL.md). If it's a routine same-pattern model addition, docs likely don't need touching — don't update them reflexively.
