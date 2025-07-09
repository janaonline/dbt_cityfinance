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

## 🏷 How to Add Tags in Models

```sql
{{ config(
    materialized='table',
    tags=['property_tax', 'marts']
) }}
```

---

## 🚀 Common dbt Run Commands with Tags

### ✅ Run all models from a module:

```bash
dbt run --select tag:property_tax
```

### ✅ Run only staging models from grants\_condition:

```bash
dbt run --select grants_condition tag:staging
```

### ✅ Run only marts (dashboard-ready) models:

```bash
dbt run --select tag:marts
```

---

## 🔁 Use Path Selectors for Precision

```bash
dbt run --select models/property_tax/staging tag:staging
```

---

## 🔀 Use `--target` to Switch Between Environments

Your `profiles.yml` may contain:

```yaml
outputs:
  staging:
    schema: mongo_staging
    ...
  prod:
    schema: CF_Prod
    ...
```

To run in different environments:

```bash
# Run in staging
dbt run --target staging

# Run in production
dbt run --target prod
```

💡 You can combine with tags:

```bash
dbt run --select tag:grants_condition tag:marts --target prod
```

---

## ⛔ Avoid This

```bash
dbt run --select tag:staging
```

❌ Will run all staging models from all modules — not filtered.

---

---

## ⚠️ How to Run a Single Model Like `fold1Summary.sql`

Running `dbt run --select tag:grants_condition tag:marts` will run **all models** tagged with either.

To run just **one model**, do this:

### ✅ Option 1: Use model name (same as filename without `.sql`)

```bash
dbt run --select fold1Summary --target prod
```

### ✅ Option 2: Use exact file path

```bash
dbt run --select models/grants_condition/marts/fold1Summary.sql --target prod
```

### ✅ Option 3: Combine model name and tag

```bash
dbt run --select fold1Summary tag:grants_condition --target prod
```

### 🔍 Preview before running:

```bash
dbt ls --select fold1Summary
```

---

## ✅ Best Practices

| Best Practice                                            | Why                                   |
| -------------------------------------------------------- | ------------------------------------- |
| Use tags to group by module + layer                      | Helps with maintenance and debugging  |
| Add `--target` to run for a specific environment         | Keeps dev, staging, and prod separate |
| Combine tags and paths                                   | Avoids accidental model runs          |
| Generate docs with `dbt docs generate && dbt docs serve` | See lineage and tags visually         |

---

## 🔍 Bonus Commands

```bash
dbt ls --select tag:grants_condition     # Preview models to be run
dbt test --select tag:property_tax       # Run only tests for a tag
dbt run --select tag:property_tax --target prod   # Run in prod
```
