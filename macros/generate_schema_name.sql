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
--                        none, the value is returned directly.
--   node               - (object) dbt node object (passed by dbt when used as a
--                        generator hook). This macro does not currently use
--                        node, but it's accepted for compatibility with dbt.
--
-- Behavior / Important notes:
--   - If custom_schema_name is not none, the macro returns it AS IS. This means
--     an empty string ('') is returned unchanged — to use the fallback, pass
--     None (or omit the argument) rather than an empty string.
--   - The macro does not quote or validate the schema identifier. Ensure the
--     returned string is a valid schema name in your target database.
--   - This macro is intentionally simple to allow projects to override behavior
--     (for example to inject environment or git branch information into schema).
--
-- Examples:
--   {{ generate_schema_name('analytics', node) }}  -> 'analytics'
--   {{ generate_schema_name(None, node) }}         -> target.schema (e.g. 'dev')
--
{% macro generate_schema_name(custom_schema_name, node) %}
    {{ 
        custom_schema_name 
        if custom_schema_name is not none 
        else target.schema 
    }}
{% endmacro %}