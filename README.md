# 📊 DBT for [CityFinance](https://cityfinance.in/)

## 🧭 Index

- [🔧 Using the Project](#-using-the-project)
- [📚 Resources](#-resources)
- [🧾 DBT Tagging, Selector & Environment Reference Guide](#-dbt-tagging-selector--environment-reference-guide)
  - [✅ Why Use Tags?](#️-why-use-tags)
  - [📁 Folder Structure (Organized by Module)](#-folder-structure-organized-by-module)
  - [🏷️ Tags Setup (`dbt_project.yml`)](#️-tags-setup-dbt_projectyml)
  - [✅ Run Models by Tag](#️-run-models-by-tag)
  - [🧪 For Dev vs Prod Environments](#-for-dev-vs-prod-environments)
  - [🧠 How Schema Naming Is Controlled](#-how-schema-naming-is-controlled)
  - [🚀 Optional: Prefect/Dalgo Task Commands](#-optional-prefectdalgo-task-commands)
  - [🧼 Clean-Up & Best Practices](#-clean-up--best-practices)
- [🌱 How to Load Data from CSV Files Using dbt Seed](#-how-to-load-data-from-csv-files-using-dbt-seed)
  - [⚙️ Configure Seeds Schema in dbt_project.yml](#️-configure-seeds-schema-in-dbt_projectyml)
  - [🌱 How to Load the Same Seed into Multiple Schemas](#-how-to-load-the-same-seed-into-multiple-schemas)

---

## 🔧 Using the Project


Follow these steps to set up the project locally:

### 1. Clone the Repository

```bash
git clone <repo-url>
cd dbt_cityfinance
```

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate   # On Mac/Linux
venv\Scripts\activate      # On Windows (PowerShell)
```

### 3. Upgrade pip

```bash
pip install --upgrade pip
```

### 4. Install dbt Core + Postgres Adapter

```bash
pip install dbt-core dbt-postgres
```

> Replace `dbt-postgres` with the adapter you need if using a different warehouse
> (e.g., `dbt-snowflake`, `dbt-bigquery`, `dbt-duckdb`, etc.)

### 5. Verify Installation

```bash
which python3     # should point to ./venv/bin/python3
which dbt         # should point to ./venv/bin/dbt
dbt --version     # should show installed + latest version
```

### 6. Run dbt

```bash
dbt debug         # check connection + setup
dbt run           # run models
dbt test          # run tests
```

## 📚 Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


# 🧾 DBT Tagging, Selector & Environment Reference Guide

---

## ✅ Why Use Tags?

Tags help you:

* Organize models by module (e.g., `property_tax`, `grants_condition`)
* Identify model layer (e.g., `staging`, `marts`)
* Filter model runs for dev, testing, or CI/CD pipelines


## 📁 Folder Structure (Organized by Module)

Your DBT repo structure should look like:

---

## 📁 Recommended Folder & Tag Structure

```bash
models/
├── property_tax/
│   ├── staging/
│   │   └── stg_property_tax_data.sql      → tags: ['property_tax', 'staging']
│   └── marts/
│       └── mart_property_summary.sql      → tags: ['property_tax', 'marts']
│   └── source.yml
│   └── schema.yml
├── grants_condition/
│   ├── staging/
│   │   └── stg_grants_condition.sql       → tags: ['grants_condition', 'staging']
│   └── marts/
│       └── mart_grants_summary.sql        → tags: ['grants_condition', 'marts']
│   └── source.yml
│   └── schema.yml
```

> This tree is illustrative. For this repo's actual, current folder layout and per-module inventory (which modules really have `staging/`/`schema.yml`), see **[docs/folder_structure.md](docs/folder_structure.md)**. For how schema routing, materialization, tags, and data flow actually work end-to-end, see **[docs/architecture.md](docs/architecture.md)**.

---

## 🏷️ Tags Setup (`dbt_project.yml`)

Use `+tags` in `dbt_project.yml` like this:

```yaml
models:
  Janaagraha:  # match your project name
    property_tax_poc:
      +schema: property_tax_poc_prod
      +tags: ['property_tax']
    grants_condition:
      staging:
        +schema: grants_condition_staging
        +tags: ['grants_condition', 'staging']
        +materialized: table
      marts:
        +schema: grants_condition_prod
        +tags: ['grants_condition', 'marts']
        +materialized: table
  elementary:
    +schema: "elementary"
```

---

## ✅ Run Models by Tag

| Command                                                                    | What It Does                                            |
| --------------------------------------------------------------------------- | ------------------------------------------------------- |
| `dbt run --select tag:property_tax --target dev`                            | Runs all models tagged `property_tax`                   |
| `dbt run --select tag:grants_condition --target dev`                        | Runs all models from grants condition module            |
| `dbt run --select tag:staging --target dev`                                 | ⚠️ Runs *all* staging models across modules             |
| `dbt run --select models/grants_condition/staging tag:staging --target dev` | ✅ Best way to run only grants\_condition staging models |
| `dbt run --select models/grants_condition/marts tag:marts --target dev`     | Runs only `marts` folder models for `grants_condition`  |

`--target dev` is shown above since it's the safe default (lands in the sandbox `dev_schema`) — swap it for `--target prod` when you deliberately want the real per-module schema instead. See **[🧪 For Dev vs Prod Environments](#-for-dev-vs-prod-environments)** below for the full explanation.

---

## 🧪 For Dev vs Prod Environments

You have only **one database** (`cityfinance`, on the shared production RDS instance) — there's no separate dev database. Schema separation is handled entirely by **which named target you run under**, combined with the custom macro described below.

### Local `profiles.yml` — two targets, same database

```yaml
Janaagraha:
  target: dev   # 👈 default when --target is omitted
  outputs:
    dev:
      type: postgres
      ...
      schema: dev_schema   # only ever used when target.name == 'dev' — see below
    prod:
      type: postgres
      ...
      schema: dev_schema   # NOTE: this value is never actually read — see below
```

- **`dev`** — the profile's default. Both a bare `dbt run` (no `--target` flag) and an explicit `--target dev` resolve to this target, and both land in the shared `dev_schema` sandbox.
- **`prod`** — must be requested explicitly with `--target prod`. Lands in each module's *real* configured `+schema` from `dbt_project.yml` (e.g. `property_tax_poc_prod`, `cf_requests`) — the same schemas Dalgo's production orchestration writes to.

Dalgo (production orchestration) supplies its own separate internal `profiles.yml`/target — not this file, not visible to this repo — so don't confuse Dalgo's target naming with the local `dev`/`prod` targets described here.

---

## 🧠 How Schema Naming Is Controlled

The actual macro in this project (`macros/generate_schema_name.sql`):

```jinja
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- if target.name == 'dev' -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name if custom_schema_name is not none else target.schema }}
    {%- endif -%}
{% endmacro %}
```

dbt calls this automatically for **every model** (it's one of dbt's reserved macro-override hooks — no model ever references it directly). It's given `custom_schema_name`, which is whatever `+schema` is set to for that model's module in `dbt_project.yml`, and it has access to `target.name` (the current target's name, e.g. `'dev'` or `'prod'`) and `target.schema` (the `schema:` value from that target's block in `profiles.yml`).

**The key thing to understand:** `target.schema` (the `schema:` field in `profiles.yml`) is not automatically used anywhere — it's only used if the macro's code explicitly returns it. Whether that happens depends entirely on `target.name`, not on what the `schema:` field says.

### Worked example: `tax_sub_rate` (module `property_tax_poc`, `+schema: property_tax_poc_prod`)

| Command | `target.name` | Branch taken | Result |
|---|---|---|---|
| `dbt run --select tax_sub_rate --target dev` | `'dev'` | `if` branch → returns `target.schema` | `dev_schema` |
| `dbt run --select tax_sub_rate` *(no `--target` flag)* | `'dev'` (the profile default) | `if` branch → returns `target.schema` | `dev_schema` |
| `dbt run --select tax_sub_rate --target prod` | `'prod'` | `else` branch → `custom_schema_name` is `'property_tax_poc_prod'`, not `None` → returns it | `property_tax_poc_prod` |

Notice the `prod` target's `schema: dev_schema` line in `profiles.yml` is **never actually read** in that third row — the `else` branch checks `custom_schema_name` first, finds it's not `None` (every module sets `+schema` explicitly), and returns that instead. `target.schema` would only be used as a last-resort fallback if some module forgot to set `+schema` at all, which doesn't happen today. So it doesn't matter that both `dev:` and `prod:` blocks happen to write `schema: dev_schema` — only the `dev:` block's value is ever actually consulted, because only the `dev`-named target's branch reads it.

✅ Net effect:
* `--target dev` or no flag at all → safe `dev_schema` sandbox.
* `--target prod` → the real `+schema` exactly as written in `dbt_project.yml`, no `<target>_<custom>` prefixing.

---

## 🚀 Optional: Prefect/Dalgo Task Commands

Make sure Dalgo/Prefect tasks **run this before `dbt run`:**

```bash
dbt deps
```

To install packages from `packages.yml`.

---

## 🧼 Clean-Up & Best Practices

| Item           | Best Practice                                        |
| -------------- | ---------------------------------------------------- |
| `profiles.yml` | Only keep `dev` locally. Dalgo uses its own          |
| Schema logic   | Always declare `+schema:` in `dbt_project.yml`       |
| Tags           | Use `+tags:` to organize & control CLI execution     |
| Macros         | Place `generate_schema_name` in `macros/`            |
| Logs           | Ignore `target/` and `dbt_packages/` in `.gitignore` |


---

## 🌱 How to Load Data from CSV Files Using dbt Seed

If you want to insert data from a `.csv` file (for example, `iso_codes.csv`), use the `dbt seed` command:

```bash
dbt seed --select iso_codes
```

- This will load the data from `seeds/iso_codes.csv` into your database as a table named `iso_codes`.
- Make sure your CSV file is placed in the `seeds/` directory of your dbt project.
- You can reference this table in your models using `{{ ref('iso_codes') }}`.

**Tip:**  
You can use `dbt seed` for any static or reference data you want to manage with version control and load into your warehouse.

---

### ⚙️ Configure Seeds Schema in dbt_project.yml

To ensure your seed data (like `iso_codes.csv`) is loaded into the correct schema, add the following to your `dbt_project.yml` file:

```yaml
seeds:
  Janaagraha:
    +schema: cf_prod
```

This will make dbt load all seed files into the `CF_Prod` schema for

---

### 🌱 How to Load the Same Seed into Multiple Schemas

dbt seeds can only load each CSV into one schema per run.  
If you want the same seed data (like `iso_codes.csv`) available in multiple schemas, use a model to copy it after seeding:

1. **Seed into your primary schema (e.g., `CF_Prod`) as shown above.**

2. **Create a model to copy the data to another schema:**

```sql
-- models/grants_condition/iso_codes.sql
{{ config(schema='cf_prod', materialized='table') }}

select * from {{ source('cityfinance','iso_codes') }}
```

```bash
dbt run --select iso_codes
```

- This will create a table named `iso_codes_copy` in the `grants_condition_prod` schema with the same data.
- You can rename the model or table as needed.

**Tip:**  
This approach keeps your seed data DRY and avoids duplicating CSV files.

---