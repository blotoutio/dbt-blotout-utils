{% macro get_id_map_query() %}
    {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
    {%- if source_relation != none %}
        {% set get_active_pipelines %}
        SELECT
            payload
        FROM
            {{ env_var('SCHEMA') }}.connection_pipeline {% endset %}
        {% set results = run_query(get_active_pipelines) %}
        {%- if execute %}
            {% set payloadList = results.columns [0].values() %}
        {% else %}
            {% set payloadList = [] %}
        {% endif -%}

        {%- if payloadList | length > 0 %}
            {%- for payload in payloadList %}
                {% set payloadObj = fromjson(payload) %}
                {% set streams = payloadObj ['syncCatalog'] ['streams'] %}
                {% for stream in streams %}
                    {% set table_name = stream ['stream'] ['name'] %}
                    {% set id_mapping = stream ['stream'] ['jsonSchema'] ['metadata'] ['id_mapping'] %}
                    {%- if id_mapping | length > 0 %}
                        {%- for id_map_definition in id_mapping %}
                            {% set map_provider = id_map_definition ['map_provider'] %}
                            {% set map_columns = id_map_definition ['map_column'] %}
                            {%- for map_column in map_columns %}
                                 UNION
                                 SELECT
                                     DISTINCT {{ map_column }},
                                     {{ map_provider }} AS data_map_id,
                                     '{{ map_provider }}' AS data_map_provider,
                                     CAST(event_datetime AS timestamp) AS "user_id_created"
                                 FROM
                                     hist_table_name
                            {% endfor -%}
                        {% endfor -%}
                    {% endif -%}
                {% endfor -%}
            {% endfor -%}
        {% endif -%}
    {% endif -%}
{% endmacro %}
