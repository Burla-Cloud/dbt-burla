-- dbt-burla_bigquery inherits all SQL macros from the wrapped BigQuery adapter.
{% macro burla_bigquery__current_timestamp() %}
  {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
{% endmacro %}
