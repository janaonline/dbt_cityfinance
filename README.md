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
  - [🧪 Testing & Running Locally](#-testing--running-locally)
  - [🚀 Optional: Prefect/Dalgo Task Commands](#-optional-prefectdalgo-task-commands)
  - [🧼 Clean-Up & Best Practices](#-clean-up--best-practices)
- [🌱 How to Load Data from CSV Files Using dbt Seed](#-how-to-load-data-from-csv-files-using-dbt-seed)
  - [⚙️ Configure Seeds Schema in dbt_project.yml](#️-configure-seeds-schema-in-dbt_projectyml)
  - [🌱 How to Load the Same Seed into Multiple Schemas](#-how-to-load-the-same-seed-into-multiple-schemas)

---

## 🔧 Using the Project

Try running the following commands:

```bash
dbt run
dbt test
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

| Command                                                        | What It Does                                            |
| -------------------------------------------------------------- | ------------------------------------------------------- |
| `dbt run --select tag:property_tax`                            | Runs all models tagged `property_tax`                   |
| `dbt run --select tag:grants_condition`                        | Runs all models from grants condition module            |
| `dbt run --select tag:staging`                                 | ⚠️ Runs *all* staging models across modules             |
| `dbt run --select models/grants_condition/staging tag:staging` | ✅ Best way to run only grants\_condition staging models |
| `dbt run --select models/grants_condition/marts tag:marts`     | Runs only `marts` folder models for `grants_condition`  |

---

## 🧪 For Dev vs Prod Environments

You have only **one database**, so schema separation is handled inside `dbt_project.yml`.

But your `profiles.yml` still controls which database/schema gets used (default fallback):

### Example local `profiles.yml`:

```yaml
janaagraha:
  target: dev
  outputs:
    dev:
      type: postgres
      ...
      schema: dev_schema  # fallback only; overridden by `+schema:` in project
```

### 🔁 Dalgo's Platform

Dalgo uses **its own internal `profiles.yml`**, e.g., with:

```yaml
target: dbt_staging
```

So all models without `+schema` would go into `dbt_staging`.

But you **override that** with:

```yaml
+schema: grants_condition_prod
```

---

## 🧠 How Schema Naming Is Controlled

You overrode the default schema naming logic using:

```jinja
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{% endmacro %}
```

✅ So now:

* `+schema: grants_condition_prod` → will generate schema **exactly as written**
* No more unwanted prefixes like `dbt_staging_grants_condition_prod`

---

## 🧪 Testing & Running Locally

Temporarily Remove or Comment Out `+schema:` in `dbt_project.yml`

1. In your `dbt_project.yml`, comment out or remove the `+schema:` line under the relevant model path (e.g., under `grants_condition.marts`).
2. Save the file.
3. Run: 

### Run only property_tax module:

```bash
dbt run --select grants_condition --target dev
```
or Run only property_tax tag

```bash
dbt run --select tag:grants_condition --target dev
```
or Run by folder path

```bash
dbt run --select models/grants_condition/marts tag:marts
```

### Run specific sql file (e.g., fold1bUntiedAndTied.sql file)

```bash
dbt run --select fold1bUntiedAndTied --target dev
```

This will use the schema from your `profiles.yml` (dev_schema).

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