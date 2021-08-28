{% macro id_map_for_el_data() %}
    {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
    {%- if source_relation != none %}
        {%- set get_active_pipelines %}
        SELECT
             payload
        FROM
            (SELECT
                 payload,
                 name,
                 created_at,
                 updated_at,
                 is_deleted,
                 row_number()
                over (partition BY name
            ORDER BY  updated_at DESC) AS RN
            FROM {{ env_var('SCHEMA') }}.connection_pipeline) emap
        WHERE emap.RN = 1 AND is_deleted = 0
        {% endset -%}
        {%- set results = run_query(get_active_pipelines) -%}
        {%- if execute %}
            {%- set payloadList = results.columns [0].values() -%}
        {% else %}
            {%- set payloadList = [] -%}
        {% endif -%}
        {%- if payloadList | length > 0 %}
            {%- for payload in payloadList %}
                {%- set payloadObj = fromjson(payload) -%}
                {%- set streams = payloadObj ['syncCatalog'] ['streams'] -%}
                {%- for stream in streams %}
                    {%- set table_name = stream ['stream'] ['name'] -%}
                    {%- set id_mapping = stream ['stream'] ['jsonSchema'] ['metadata'] ['id_mapping'] -%}
                    {%- if id_mapping | length > 0 %}
                        {%- set map_column = [] -%}
                        {%- set map_provider = [] -%}
                        {%- set map_primary_key = [] -%}
                        {%- for id_map_definition in id_mapping %}
                            {%- if id_map_definition ['map_id'] == False %}
                                {%- set map_column = map_column.append(id_map_definition ['map_column']) -%}
                                {%- set map_provider = map_provider.append(id_map_definition ['map_provider']) -%}
                            {% else %}
                                {%- set map_primary_key = map_primary_key.append(id_map_definition ['map_column']) -%}
                            {% endif -%}
                        {% endfor -%}
                        {%- for i in range(map_column | length) %}
                            UNION
                            SELECT
                                DISTINCT {{ map_primary_key[0] }} as user_id,
                                {{ map_column[i] }} AS data_map_id,
                                '{{ map_provider[i] }}' AS data_map_provider,
                                CAST(event_datetime AS timestamp) AS "user_id_created"
                            FROM
                                -- ToDo Schema Name is missing
                                hist_{{ table_name }}
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

{% macro id_map_for_clickstream_mapping(is_incremental = false) %}
    SELECT
        emap.user_id,
        emap.data_map_id,
        emap.data_map_provider,
        CAST(
            emap.user_id_created AS TIMESTAMP
        ) AS "user_id_created"
    FROM
        (
            SELECT
                user_id,
                data_map_id,
                data_map_provider,
                user_id_created,
                ROW_NUMBER() over (
                    PARTITION BY user_id,
                    data_map_provider
                    ORDER BY
                        user_id_created DESC
                ) AS rn
            FROM
                {{ env_var('SCHEMA') }}.core_events
            WHERE
                user_id IS NOT NULL
                AND data_map_id IS NOT NULL
                {%- if is_incremental %}
                    AND CAST(user_id_created AS TIMESTAMP) > (SELECT MAX(user_id_created) FROM {{ this }})
                {% endif -%}
        ) emap
    WHERE
        emap.rn = 1
{% endmacro %}