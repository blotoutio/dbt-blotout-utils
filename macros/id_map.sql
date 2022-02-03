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
                    {%- set table_name = stream ['stream'] ['name'] | lower-%}
                    {%- set id_mapping = stream ['stream'] ['jsonSchema'] ['metadata'] ['id_mapping'] -%}
                    {%- if id_mapping | length > 0 %}
                        {%- set map_column = [] -%}
                        {%- set map_provider = [] -%}
                        {%- set map_primary_key = [] -%}
                        {%- set map_primary_provider = [] -%}
                        {%- for id_map_definition in id_mapping %}
                            {%- if id_map_definition ['map_id'] == False %}
                                {%- set map_column = map_column.append(id_map_definition ['map_column']) -%}
                                {%- set map_provider = map_provider.append(id_map_definition ['map_provider']) -%}
                            {% else %}
                                {%- set map_primary_key = map_primary_key.append(id_map_definition ['map_column']) -%}
                                {%- set map_primary_provider = map_primary_provider.append(id_map_definition ['map_provider']) -%}
                            {% endif -%}
                        {% endfor -%}
                        {%- for i in range(map_column | length) %}
                            {%- set check_relation = adapter.get_relation(
                                     database = env_var('DATABASE'),
                                     schema = sourceName,
                                     identifier = table_name)
                                -%}
                            {%- set elt_table_col = adapter.get_columns_in_table(schema= sourceName, identifier=table_name) -%}
                            {%- set elt_table_col_name = [] -%}
                             {%- for elt_col in elt_table_col %}
                                 {%- set elt_table_col_name = elt_table_col_name.append(elt_col.name) %}
                            {%- endfor -%}
                            {% if check_relation != None and blotout_utils.camel_to_snake(map_column[i]) in elt_table_col_name
                                and blotout_utils.camel_to_snake(map_primary_key[0]) in elt_table_col_name %}
                                UNION
                                SELECT
                                    trim("{{ blotout_utils.camel_to_snake(map_column[i]) }}") AS user_id,
                                    trim("{{ blotout_utils.camel_to_snake(map_primary_key[0]) }}") as data_map_id,
                                    '{{ map_provider[i] }}' AS "user_provider",
                                    '{{ map_primary_provider[0] }}' AS data_map_provider,
                                    CAST(min(etl_run_datetime) AS timestamp) AS "user_id_created"
                                FROM
                                    {{ sourceName }}.{{ table_name }}
                                WHERE
                                    "{{ blotout_utils.camel_to_snake(map_column[i]) }}" IS NOT NULL
                                    AND "{{ blotout_utils.camel_to_snake(map_primary_key[0]) }}" IS NOT NULL
                                    AND trim("{{ blotout_utils.camel_to_snake(map_column[i]) }}") NOT IN ('nan', '')
                                    AND trim("{{ blotout_utils.camel_to_snake(map_primary_key[0]) }}") NOT IN ('nan', '')
                                {%- if is_incremental %}
                                     AND CAST(etl_run_datetime AS timestamp) >
                                        (SELECT COALESCE(MAX(user_id_created), cast('1970-01-01 00:00:00.000' as timestamp))
                                            FROM {{ this }} WHERE orig_user_provider = '{{ map_provider[i] }}')
                                {% endif %}
                                GROUP BY "{{ blotout_utils.camel_to_snake(map_primary_key[0]) }}", "{{ blotout_utils.camel_to_snake(map_column[i]) }}"
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
        {%- if "search_gclid" == column.name %}
            UNION
            SELECT
                user_id,
                search_gclid AS data_map_id,
                MAX(application_name) AS "user_provider",
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

        {%- if "search_fbclid" == column.name %}
            UNION
            SELECT
                user_id,
                search_fbclid AS data_map_id,
                MAX(application_name) AS "user_provider",
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

        {%- if "search_twclid" == column.name %}
            UNION
            SELECT
                user_id,
                search_twclid AS data_map_id,
                MAX(application_name) AS "user_provider",
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

        {%- if "persona_email" == column.name %}
            UNION
            SELECT
                user_id,
                search_twclid AS data_map_id,
                MAX(application_name) AS "user_provider",
                'twclid' AS data_map_provider,
                MIN(CAST(event_datetime AS timestamp)) AS "user_id_created"
            FROM
                core_events
            WHERE
                search_twclid IS NOT NULL
                AND user_id IS NOT NULL
                {% if is_incremental %}
                    AND CAST(user_id_created AS timestamp) > (select max(user_id_created) FROM {{ this }})
                {% endif %}
                  GROUP BY user_id, search_twclid
        {% endif -%}

    {%- endfor %}
{% endmacro %}

{% macro id_map_for_clickstream_mapping(is_incremental = FALSE) %}
    SELECT
        emap.user_id,
        emap.data_map_id,
        application_name AS "user_provider",
        emap.data_map_provider,
        CAST(
            emap.user_id_created AS TIMESTAMP
        ) AS "user_id_created"
    FROM
        (
            SELECT
                user_id,
                data_map_id,
                application_name,
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