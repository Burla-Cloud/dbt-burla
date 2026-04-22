-- dbt-burla inherits all SQL macros from the wrapped warehouse adapter.
-- Everything dbt asks for via adapter.dispatch(...) that is declared in the
-- parent adapter's macros package is resolved through dbt's macro dispatch
-- on the adapter's `type()`. We redirect every `burla_duckdb`-prefixed
-- lookup to the corresponding `duckdb` macro.

{% macro burla_duckdb__current_timestamp() %}
  {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
{% endmacro %}
