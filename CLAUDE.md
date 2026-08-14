# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is a dbt (dbt-core 1.11, dbt-postgres) project for [CityFinance](https://cityfinance.in/), a Janaagraha initiative. It transforms raw operational data — originally captured in a MongoDB-backed app and replicated into a single shared Postgres database (`cityfinance`) — into per-module reporting/analysis tables (property tax, grants, AFS financial diagnosis, market readiness, etc.) consumed downstream for reporting/export.

## Setup & common commands

```bash
source venv/bin/activate      # local venv already has dbt-core + dbt-postgres
dbt deps                      # install packages from packages.yml — run before any dbt run
dbt debug                     # verify profiles.yml connection
dbt run --target dev          # lands in the shared dev_schema sandbox (also the default if --target is omitted)
dbt test --target dev
dbt seed --target dev         # loads seeds/iso_codes.csv
dbt build --target dev        # run + test in DAG order
```

**`dev` is the safe default.** `profiles.yml`'s default target (used when `--target` is omitted) is `dev`, so both a bare `dbt run` and an explicit `--target dev` land in `dev_schema`. To deliberately verify a model against its real configured `+schema` (e.g. `property_tax_poc_prod`) — the same schemas Dalgo's production runs write to, on the same shared database — pass `--target prod` explicitly.

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

`profiles.yml` is git-ignored and holds real DB credentials — never commit it. It defines two targets against the same single shared Postgres instance: `dev` (schema `dev_schema` — the default, used both when `--target` is omitted and when passed explicitly) and `prod` (must be requested explicitly via `--target prod`, resolves through each module's real `+schema`). This repo's Dalgo-orchestrated production runs are unrelated to this local `prod` target — Dalgo supplies its own internal `profiles.yml`/target entirely separate from this file; don't assume this `profiles.yml` exists when reasoning about Dalgo's runs.

## Architecture

Models are organized **one folder per business module** under `models/`, not by dbt's usual staging/marts-at-the-top layout:

```
models/<module>/
  source.yml       # source declarations (raw tables replicated from MongoDB)
  schema.yml        # (optional) column tests
  staging/          # (optional) thin cleanup/rename models
  marts/            # final, table-materialized models with business logic
```

Current modules: `property_tax_poc`, `grants_condition`, `grants_allocation`, `ap_api_poc`, `afs_digitisation_tracker`, `afs_analysis`, `market_readiness`, `nmam_ulb_response`, `cf_municipal_finance_rt`. Most modules only have `marts/`; `property_tax_poc` is the one with a full `staging/` layer and, along with `cf_municipal_finance_rt`, the only ones with a `schema.yml`.

**Schema routing is entirely config-driven, not folder-driven.** Every module needs its own `models: Janaagraha: <module>: { staging: {...}, marts: {...} }` block in `dbt_project.yml` setting `+schema`, `+tags`, and `+materialized` (almost everything here is `table`). `macros/generate_schema_name.sql` returns `+schema` **verbatim** (no `<target>_<custom>` concatenation) — omitting `+schema` falls back to `target.schema` rather than erroring. **Exception:** under `--target dev` (also this local `profiles.yml`'s default, used when `--target` is omitted), the macro ignores `+schema` entirely and always resolves to `dev_schema` — the safe sandbox on the shared database. Any other target — this local `profiles.yml`'s explicit `prod` target, or Dalgo's separate production orchestration — uses `+schema` verbatim as usual.

Full data flow, the complete schema-routing table, macro behavior, recurring SQL patterns (CAGR, JSON line-item extraction, fuzzy ULB joins, output-aliasing conventions), and testing architecture are documented in **[docs/architecture.md](docs/architecture.md)**. Full repo layout and per-module inventory (which modules have `staging/`/`schema.yml`) are in **[docs/folder_structure.md](docs/folder_structure.md)**.

## Packages (`packages.yml`)

`dbt_utils` 1.3.0, `dbt_expectations` 0.10.4 (installed, currently unused), `elementary-data/elementary` 0.16.2 (own `elementary` schema). The project-level flag `require_explicit_package_overrides_for_builtin_materializations: false` exists specifically for Elementary/dbt 1.8+ compatibility — don't remove it.

## Claude Skills

Repo-specific workflows live in `.claude/skills/`: `dbt-model-development`, `dbt-module-development`, `sql-review`, `sql-business-logic`, `dbt-testing`, `dbt-debugging`, `dbt-change-review`, `dbt-documentation-maintenance`. Use the matching skill instead of re-deriving conventions from scratch — e.g. adding a model → `dbt-model-development`, reviewing SQL → `sql-review`.

## Documentation and Architecture Maintenance

After completing a task, check whether it changed a documented fact — update only what actually changed, don't touch docs reflexively:

- Folder/file layout changed → update `docs/folder_structure.md`.
- Schema routing, materialization, tags, macros, SQL conventions, testing strategy, or dependencies changed → update `docs/architecture.md`.
- A project-wide instruction or command changed → update this file.
- A reusable workflow or domain-knowledge detail changed → update the relevant skill in `.claude/skills/`.
- A new module or model was added → review `docs/folder_structure.md`, `docs/architecture.md`'s module/routing tables, and `dbt-module-development`.
- SQL or business logic changed → review `sql-business-logic`, relevant tests, and `docs/architecture.md`.

See `.claude/skills/dbt-documentation-maintenance/SKILL.md` for the full decision guide.
