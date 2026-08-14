---
name: dbt-testing
description: Add or review dbt tests in this project (dbt_cityfinance). Use when asked to add tests for a model/column, or to assess test coverage — this project's testing setup is minimal and generic-tests-only, not the dbt_expectations-heavy setup you might expect from the installed packages.
---

# dbt testing (dbt_cityfinance)

## Current state (verify against the repo before assuming this is still accurate)

Only 2 of 9 modules have a `schema.yml` at all: `models/property_tax_poc/schema.yml` (tests `stg_tax_sub_rate`: `not_null` on 4 columns) and `models/cf_municipal_finance_rt/schema.yml` (tests 4 marts models with `not_null`, `accepted_values`, and one `dbt_utils.unique_combination_of_columns`). The other 7 modules (`grants_condition`, `grants_allocation`, `ap_api_poc`, `afs_digitisation_tracker`, `afs_analysis`, `market_readiness`, `nmam_ulb_response`) have **no declared tests**. There are no custom singular tests (`tests/` dir is empty/`.gitkeep` only), no snapshots, no seed-level tests.

`dbt_expectations` 0.10.4 is installed (declared in `packages.yml`) but **not used anywhere** — don't assume it's already wired into a workflow; if you use it, you're introducing the project's first usage.

## Adding tests to an existing schema.yml

Follow the exact style in `models/cf_municipal_finance_rt/schema.yml`:

```yaml
- name: <column>
  description: "<what this column is and why the test applies>"
  tests:
    - not_null
    - accepted_values:
        arguments:
          values: ['Value1', 'Value2']
```

For a model-level test (not tied to one column):
```yaml
tests:
  - dbt_utils.unique_combination_of_columns:
      arguments:
        combination_of_columns:
          - ulb
          - state
          - year
```

Notably, `cf_municipal_finance_rt/schema.yml` uses `# TODO: ...` comments in place of a test when a test is *intentionally deferred* (e.g. "add not_null once legend match-rate against production data is verified") rather than silently having no test. Follow this convention — if you decide a column needs a test later rather than now, leave a `# TODO` explaining why, don't just omit it silently.

## Creating a schema.yml for a module that doesn't have one

Only add one when there's a genuine test to write — don't create an empty `schema.yml` speculatively. Model the file on `models/cf_municipal_finance_rt/schema.yml`'s structure (`version: 2`, `models:` list, per-model `description`, per-column `description` + `tests`).

## Running tests

```bash
dbt test --select <model_name> --target dev
dbt test --select tag:<module> --target dev   # module-wide, where tags are actually configured — see architecture.md's routing-table caveats
dbt build --select tag:<module> --target dev  # run + test together
```

## Judgment calls

- Prefer a generic test (`not_null`, `unique`, `accepted_values`, `dbt_utils.unique_combination_of_columns`) over reaching for `dbt_expectations` — it's what every existing test in the project uses, and introducing the first `dbt_expectations` usage is a bigger decision than adding a routine not_null test.
- Don't retrofit tests onto unrelated columns while working on a specific model — scope test additions to what the current task actually touched, consistent with [dbt-change-review](../dbt-change-review/SKILL.md).
- If you find yourself wanting a test that can't be expressed generically (e.g. "total_validations is always exactly 22 by construction" — flagged as a `# TODO` for a custom singular test in `cf_municipal_finance_rt/schema.yml`), that's the situation `tests/` (currently empty) exists for — a custom singular SQL test file, not a schema.yml addition.
