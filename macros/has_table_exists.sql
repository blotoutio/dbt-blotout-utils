{% macro has_table_exists(database_name,schema_name,table_name) %}

    {%- set metadata_source_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) -%}

    {% if metadata_source_relation %}
        {{ log("dbt-blotout-utils:output 1", info=True) }}
    {% else %}
        {{ log("dbt-blotout-utils:output 0", info=True) }}
    {% endif %}

{% endmacro %}
