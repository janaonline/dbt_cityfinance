DBT for [cityfinance](https://cityfinance.in/) 

### Using the project

Try running the following commands:
- dbt run
- dbt test


### Resources:
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

### Build all grants\_condition models in dev:

```bash
dbt build --select grants_condition --target dev
```

### Run only property\_tax module:

```bash
dbt run --select tag:property_tax
```

or 

```bash
dbt run --select grants_condition
```

### Run grants\_condition marts only:

```bash
dbt run --select models/grants_condition/marts tag:marts
```

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
