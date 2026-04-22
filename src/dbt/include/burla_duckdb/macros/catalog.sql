-- Delegate catalog queries to the underlying duckdb adapter.
{% macro burla_duckdb__get_catalog(information_schemas, schemas) %}
    {{ return(adapter.dispatch('get_catalog', 'dbt')(information_schemas, schemas)) }}
{% endmacro %}

{% macro burla_duckdb__get_catalog_relations(information_schemas, relations) %}
    {{ return(adapter.dispatch('get_catalog_relations', 'dbt')(information_schemas, relations)) }}
{% endmacro %}
