-- Macro: design_year_minus
-- Purpose:
--   Return a design-year string N years earlier for inputs in the format 'YYYY-YY'.
--   Example: design_year_minus('uy.design_year', 1) when uy.design_year = '2022-23' -> '2021-22'
--
-- Usage:
--   {{ design_year_minus('uy.design_year', 1) }}
--
-- Arguments:
--   design_year_col - (string) SQL expression for the design year column (e.g. 'uy.design_year')
--   n               - (int) number of years to subtract
--
-- Important:
--   - Input must be in 'YYYY-YY' format. The macro performs substring + integer arithmetic and will error if the format is invalid.
--   - Pass the column/expression as a quoted string so the macro injects the SQL literal (see usage above).
{% macro design_year_minus(design_year_col, n) -%}
(
  (substring({{ design_year_col }} from 1 for 4)::int - {{ n }})::text
  || '-'
  || lpad(((substring({{ design_year_col }} from 6 for 2)::int - {{ n }})::text), 2, '0')
)
{%- endmacro %}