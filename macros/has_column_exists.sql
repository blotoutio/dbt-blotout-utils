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


{% macro select_column_if_not_exists(source_columns, column_name, as_column_name) %}
    {%- set source_columns_names = [] -%}
    {%- for column in source_columns %}
         {%- if column.name == column_name %}
            {%- set source_columns_names = source_columns_names.append(True) -%}
         {% endif -%}
    {%- endfor %}
     {%- if ((source_columns_names | length > 0) and (source_columns_names[0])) %}
        {{ column_name }} AS "{{ as_column_name }}"
     {% else -%}
        CAST(null AS varchar) AS "{{ as_column_name}}"
     {% endif -%}
{% endmacro -%}