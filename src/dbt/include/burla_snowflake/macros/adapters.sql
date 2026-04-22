-- dbt-burla_snowflake inherits all SQL macros from the wrapped Snowflake adapter.
{% macro burla_snowflake__current_timestamp() %}
  {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
{% endmacro %}
