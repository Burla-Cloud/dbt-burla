{#
  dbt-duckdb's default `table` materialization creates an intermediate
  relation (`{identifier}__dbt_tmp`) and renames it to the target after
  `submit_python_job` returns. That works for SQL-only DuckDB and for
  adapter-provided Python (DuckDB runs Python in-process, writing to the
  intermediate), but BurlaPythonJobHelper writes to the *final* target
  via the warehouse backend.

  For SQL models we keep the default DuckDB intermediate/rename flow via
  `duckdb__create_table_as`. For Python models we skip the intermediate
  entirely and let the helper write straight to `target_relation`.
#}
{% materialization table, adapter="burla_duckdb", supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {% set grant_config = config.get('grants') %}

  {%- if language == 'sql' -%}
    {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
    {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
    {{ drop_relation_if_exists(preexisting_backup_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% call statement('main', language=language) -%}
      {{- create_table_as(False, intermediate_relation, compiled_code, language) }}
    {%- endcall %}

    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    {% do persist_docs(target_relation, model) %}

    {{ adapter.commit() }}
    {{ drop_relation_if_exists(backup_relation) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

  {%- else -%}
    {# Python: write directly to target_relation via BurlaPythonJobHelper #}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if existing_relation is not none %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}

    {% call statement('main', language=language) -%}
      {{ compiled_code }}
    {%- endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    {% do persist_docs(target_relation, model) %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

  {%- endif -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
