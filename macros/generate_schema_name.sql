-- Macro: generate_schema_name
-- Purpose:
--   Return a schema name to use for model materialization. If a custom schema
--   name is provided, that value is returned; otherwise the dbt target.schema
--   is used as a fallback.
--
-- Usage:
--   This is typically used by dbt when a project overrides the generate_schema_name
--   macro, or can be called directly in macros/models:
--     {{ generate_schema_name(custom_schema_name, node) }}
--
-- Arguments:
--   custom_schema_name - (string | none) a custom schema name to use; if not
--                        none, the value is returned directly (except under the
--                        `dev` target — see dev-target override below).
--   node               - (object) dbt node object (passed by dbt when used as a
--                        generator hook). This macro does not currently use
--                        node, but it's accepted for compatibility with dbt.
--
-- Dev-target override (IMPORTANT):
--   `profiles.yml`'s only local target, `dev`, connects to the same shared
--   Postgres instance/database used everywhere else (production runs go
--   through Dalgo's own internal profiles.yml/target, not this one). Every
--   module in dbt_project.yml sets +schema explicitly, so without this
--   override every `dbt run --target dev` would write straight into the real
--   production-named schemas (e.g. property_tax_poc_prod) — profiles.yml's
--   `schema: dev_schema` would never actually be used.
--   To prevent that, when target.name == 'dev' this macro ALWAYS returns
--   target.schema (dev_schema), ignoring custom_schema_name entirely — every
--   module lands in the single shared dev_schema regardless of its configured
--   +schema. This is safe: dbt requires globally-unique model names across the
--   whole project, so there's no collision risk between modules sharing one
--   dev schema. Any other target (production via Dalgo, or any future target)
--   falls through to the original verbatim-custom_schema_name behavior,
--   completely unchanged.
--
-- Behavior / Important notes:
--   - Outside the `dev` target: if custom_schema_name is not none, the macro
--     returns it AS IS. This means an empty string ('') is returned unchanged
--     — to use the fallback, pass None (or omit the argument) rather than an
--     empty string.
--   - The macro does not quote or validate the schema identifier. Ensure the
--     returned string is a valid schema name in your target database.
--   - This macro is intentionally simple to allow projects to override behavior
--     (for example to inject environment or git branch information into schema).
--
-- Examples:
--   {{ generate_schema_name('property_tax_poc_prod', node) }}  -> 'dev_schema'         (target.name == 'dev')
--   {{ generate_schema_name('property_tax_poc_prod', node) }}  -> 'property_tax_poc_prod' (any other target)
--   {{ generate_schema_name(None, node) }}                     -> target.schema
--
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- if target.name == 'dev' -%}
        {{ target.schema }}
    {%- else -%}
        {{
            custom_schema_name
            if custom_schema_name is not none
            else target.schema
        }}
    {%- endif -%}
{% endmacro %}