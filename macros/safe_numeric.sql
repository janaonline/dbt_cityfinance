-- Macro: safe_numeric
-- Purpose:
--   Clean, validate and safely cast string expressions to numeric in SQL.
--   Centralises common logic for parsing messy source values (whitespace,
--   thousands separators, leading-dot decimals like ".4", trailing-dot "123.",
--   optional sign).
--
-- Usage:
--   Pass the column expression as a quoted string so the macro injects the SQL
--   text. Example:
--     {{ safe_numeric('ptm.value') }} AS value
--
-- Arguments:
--   col (string) - the SQL expression (e.g. 'ptm.value') to be cleaned and cast.
--
-- Behavior:
--   1. trim(col) removes leading/trailing whitespace
--   2. regexp_replace(..., '[, ]', '', 'g') removes commas and spaces (global)
--   3. Validates the cleaned string with the regex:
--        ^[-+]?(?:\d+(?:\.\d*)?|\.\d+)$
--      - allows: integers ("123"), decimals with leading zero ("0.4"),
--                 leading-dot decimals (".4"), trailing-dot ("123."), optional +/-
--      - rejects: non-numeric text, empty string, alphabetic noise
--   4. If valid, casts cleaned text to numeric; otherwise returns NULL.
--
-- Examples (input -> output):
--   '  .4 '    -> 0.4
--   '1,234.56' -> 1234.56
--   '123.'     -> 123.0
--   '' or ' '  -> NULL
--   'abc'      -> NULL
--
-- Notes:
--   - Keep using the quoted string form when calling the macro
--     ({{ safe_numeric('table.col') }}). Passing an unquoted Jinja expression
--     ({{ safe_numeric(table.col) }}) will attempt to resolve a Jinja variable.
--   - This uses Postgres regex and regexp_replace. If you run on a different
--     adapter, confirm these functions are available or adapt accordingly.
--   - If you want to accept other formats (e.g. currency symbols) pre-clean
--     them before calling this macro or extend the regex/replace logic.
--     Proposed regexp : '^[-+]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)$'
{% macro safe_numeric(col) -%}
(
  case
    when {{ col }} is not null
     and trim({{ col }}) <> ''
     and regexp_replace(trim({{ col }}), '[, ]', '', 'g') ~ '^([-+]?(\d+(.\d*)?|.\d+))$'
    then regexp_replace(trim({{ col }}), '[, ]', '', 'g')::numeric
    else null
  end
)
{%- endmacro %}