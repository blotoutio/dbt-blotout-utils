{% macro id_map_for_el_data() %}
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

{% macro id_map_for_clickstream_utm_id(source_columns, is_incremental = false) %}

    {%- for column in source_columns %}
        {%- if "search_gclid" in column.name %}
            UNION
            SELECT
                DISTINCT user_id,
                search_gclid AS data_map_id,
                'gclid' AS data_map_provider,
                CAST(event_datetime AS timestamp) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_gclid IS NOT NULL AND user_id IS NOT NULL
                {%- if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif -%}
        {% endif -%}

        {%- if "search_fbclid" in column.name %}
            UNION
            SELECT
                DISTINCT user_id,
                search_fbclid AS data_map_id,
                'fbclid' AS data_map_provider,
                CAST(event_datetime AS timestamp) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_fbclid IS NOT NULL AND user_id IS NOT NULL
                {%- if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif -%}
        {% endif -%}

        {%- if "search_twclid" in column.name %}
            UNION
            SELECT
                DISTINCT user_id,
                search_twclid AS data_map_id,
                'twclid' AS data_map_provider,
                CAST(event_datetime AS timestamp) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_twclid IS NOT NULL
                AND user_id IS NOT NULL
                {%- if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif -%}
        {% endif -%}
    {%- endfor %}
{% endmacro %}
