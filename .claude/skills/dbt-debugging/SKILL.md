---
name: dbt-debugging
description: Debug a failing or misbehaving dbt run/model in this project (dbt_cityfinance) — compile errors, wrong schema output, unexpected NULLs from safe_numeric/JSON casts, connection issues. Use when a dbt command fails or a model's output looks wrong.
---

# dbt debugging (dbt_cityfinance)

## First checks

```bash
source venv/bin/activate
dbt debug --target dev            # verify the Postgres connection actually works
dbt compile --select <model> --target dev   # catch Jinja/SQL syntax errors without hitting the DB
```

`profiles.yml` (repo root, gitignored) holds the real DB credentials — it must exist and be correctly configured locally; this repo does not create one for you and it's never committed. Production runs go through Dalgo (Prefect-based orchestration) with its own internal `profiles.yml` — don't assume local `profiles.yml` settings apply there.

**`profiles.yml`'s default target is `dev`.** A bare `dbt run`/`dbt compile` with no `--target` flag behaves identically to `--target dev` — both land in `dev_schema`. To deliberately reach each module's real `+schema` (the same production-named schemas Dalgo's runs write to, on the same shared database), you must pass `--target prod` explicitly.

## "My model landed in the wrong schema"

Schema is controlled entirely by `dbt_project.yml`'s `models: Janaagraha: <module>: { marts: { +schema: ... } } }` block — not by folder path, and not by the model's own `config()` block unless that block explicitly overrides `schema`. `macros/generate_schema_name.sql` returns `+schema` **verbatim** with no target-prefix concatenation, so:
- If `+schema` is missing from a module's block, the model falls back to `target.schema` instead of erroring.
- Check the actual `dbt_project.yml` block for the module, not what you assume it should be — see [../../docs/architecture.md](../../docs/architecture.md)'s routing table for the current expected values (that table reflects non-`dev` targets — see below for `dev`).

**Under `--target dev` (or no `--target` flag — same thing, since `dev` is the default), `+schema` is ignored entirely** — every model, in every module, lands in the single shared `dev_schema`. Only `--target prod` reaches the real production-named schema. If a model unexpectedly appears in a production-named schema instead of `dev_schema`, check whether `--target prod` was passed by mistake. If you need to confirm which schema a model resolves to without running it, `dbt parse --target prod` (or `dbt parse`/`dbt parse --target dev` for the sandbox) followed by inspecting `target/manifest.json`'s node `schema` field is a DB-connection-free way to check.

## "My `dbt run --select tag:<module>` selected nothing"

Not every module actually has working tags configured. Known gaps (verify against current `dbt_project.yml`, these could change): `property_tax_poc`'s `+tags` lines are commented out in both `staging`/`marts` blocks; `grants_condition`'s `staging:` sub-block (and its tags) is entirely commented out. Fall back to folder-path selection (`--select models/<module>`) or check the actual file before assuming the tag selector is broken.

## "A numeric column came back NULL/unexpectedly"

If the value passed through `{{ safe_numeric(col) }}`: the macro returns `NULL` for anything that fails its validation regex, by design — this is likely not a bug, but genuinely malformed source data. To debug, compile the model (`dbt compile --select <model>`) and inspect the generated SQL, or query the raw source value directly to see what actually failed validation (leading/trailing garbage, embedded text, etc). Also be aware the macro's regex is looser than its header comment describes (see architecture.md) — in the rare case a value passes when it "shouldn't" per the documented intent, that's the known discrepancy, not a new bug.

## "A JSON line-item extraction returned NULL/wrong type"

Check whether the source column is native `jsonb` or stored as `TEXT` — several models need an explicit `NULLIF(col::TEXT,'')::jsonb` cast before `->>` will work correctly. Also confirm the numeric-regex guard around the extraction (`~ '^-?[0-9]+(\.[0-9]+)?$'`) isn't silently filtering out a value that has an unexpected format (e.g. a comma or currency symbol the guard doesn't account for).

## "design_year_minus errored"

The macro requires strict `'YYYY-YY'` input and does substring + integer arithmetic with no format validation — a malformed design-year string (wrong length, non-numeric parts) will error rather than return NULL. This is intentional (fail loudly on bad data) — fix the upstream data/join, don't wrap the macro call in error suppression.

## "A fuzzy ULB join isn't matching rows I expect"

Check that **both sides** of the join apply the identical `LOWER(REGEXP_REPLACE(BTRIM(col::TEXT), '[[:space:]]+', ' ', 'g'))` normalization — a missing `BTRIM` or different whitespace handling on one side silently produces zero matches instead of erroring, since it's just a string comparison.

## Logs

`logs/dbt.log` (gitignored) holds the most recent local run's detailed log — check it for the full compiled SQL and Postgres error text when a `dbt run`/`dbt test` failure message is truncated in the terminal.
