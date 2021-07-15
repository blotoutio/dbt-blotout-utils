{% macro has_column_exists(database_name,schema_name,table_name,column_name) %}

    {%- set metadata_source_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) -%}

    {% if metadata_source_relation %}
        {%- set columns = adapter.get_columns_in_relation(metadata_source_relation) -%}
        {% set syncMode = [] %}
        {% for column in columns %}
            {% if column.name == column_name %}
                {% set syncMode = syncMode.append(1) %}
            {% endif %}
        {% endfor %}

         {% if syncMode|length > 0 %}
            {{ log("dbt-blotout-utils:output 1", info=True) }}
        {% else %}
            {{ log("dbt-blotout-utils:output 0", info=True) }}
        {% endif %}

    {% else %}
        {{ log("dbt-blotout-utils:output 0", info=True) }}
    {% endif %}

{% endmacro %}
