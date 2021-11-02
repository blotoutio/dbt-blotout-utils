{% macro id_map_for_el_data(is_incremental = False) %}
    {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
    {%- if source_relation != none %}
        {%- set get_active_pipelines %}
        SELECT
             payload,
             lower(replace(source_name, ' ', '_')) as source_name
        FROM
            (SELECT
                 payload,
                 source_name,
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
            {%- set sourceNameList = results.columns [1].values() -%}
        {% else %}
            {%- set payloadList = [] -%}
        {% endif -%}
        {%- if payloadList | length > 0 %}
            {%- for payload in payloadList %}
                {%- set sourceName = sourceNameList[loop.index-1] + '_' + env_var('ENV') -%}
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
                            {%- set check_relation = adapter.get_relation(
                                     database = env_var('DATABASE'),
                                     schema = sourceName,
                                     identifier = table_name)
                                -%}
                            {% if check_relation != None %}
                                UNION
                                SELECT
                                    {{ map_primary_key[0] }} as user_id,
                                    "{{ map_column[i] }}" AS data_map_id,
                                    '{{ map_provider[i] }}' AS data_map_provider,
                                    CAST(min(etl_run_datetime) AS timestamp) AS "user_id_created"
                                FROM
                                    {{ sourceName }}.{{ table_name }}
                                WHERE
                                    "{{ map_column[i] }}" IS NOT NULL
                                    AND "{{ map_column[i] }}" NOT IN ('nan')
                                    AND {{ map_primary_key[0] }} IS NOT NULL
                                {%- if is_incremental %}
                                     AND CAST(etl_run_datetime AS timestamp) >
                                        (SELECT COALESCE(MAX(user_id_created), cast('1970-01-01 00:00:00.000' as timestamp))
                                            FROM {{ this }} WHERE data_map_provider = '{{ map_provider[i] }}')
                                {% endif %}
                                GROUP BY {{ map_primary_key[0] }}, "{{ map_column[i] }}"
                            {% endif -%}
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
                user_id,
                search_gclid AS data_map_id,
                'gclid' AS data_map_provider,
                MIN(CAST(event_datetime AS timestamp)) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_gclid IS NOT NULL AND user_id IS NOT NULL
                {% if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif %}
                  GROUP BY user_id, search_gclid
        {% endif -%}

        {%- if "search_fbclid" in column.name %}
            UNION
            SELECT
                user_id,
                search_fbclid AS data_map_id,
                'fbclid' AS data_map_provider,
                MIN(CAST(event_datetime AS timestamp)) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_fbclid IS NOT NULL AND user_id IS NOT NULL
                {% if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif %}
                  GROUP BY user_id, search_fbclid
        {% endif -%}

        {%- if "search_twclid" in column.name %}
            UNION
            SELECT
                user_id,
                search_twclid AS data_map_id,
                'twclid' AS data_map_provider,
                MIN(CAST(event_datetime AS timestamp)) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_twclid IS NOT NULL
                AND user_id IS NOT NULL 
                {% if is_incremental %}
                    AND CAST(event_datetime AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif %}
                  GROUP BY user_id, search_twclid
        {% endif -%}
    {%- endfor %}
{% endmacro %}

{% macro id_map_for_clickstream_mapping(is_incremental = FALSE) %}
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
                    AND CAST(event_datetime AS TIMESTAMP) > (SELECT MAX(user_id_created) FROM {{ this }})
                {% endif -%}
        ) emap
    WHERE
        emap.rn = 1
{% endmacro %}