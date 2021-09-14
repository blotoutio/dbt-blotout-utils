{% macro profile_map_for_el_data() %}
    {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
        {%- set super_dict = {"id": "persona_id",
        "user_name": "persona_username",
        "first_name": "persona_firstname",
        "middle_name": "persona_middlename",
        "last_name": "persona_lastname",
        "age": "persona_age",
        "dob": "persona_dob",
        "email": "persona_email",
        "gender": "persona_gender",
        "address": "persona_address",
        "city": "persona_city",
        "state": "persona_state",
        "zip": "persona_zip",
        "country": "persona_country"}  %}
    {%- if source_relation != none %}
        {%- set get_active_pipelines %}
        SELECT
             payload,
             source_name
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
                {%- set sourceName = sourceNameList[loop.index-1] -%}
                {%- set payloadObj = fromjson(payload) -%}
                {%- set streams = payloadObj ['syncCatalog'] ['streams'] -%}
                {%- for stream in streams %}
                    {%- set mapped_column = {} -%}
                    {%- set table_name = stream ['stream'] ['name'] -%}
                    {%- set id_mapping = stream ['stream'] ['jsonSchema'] ['metadata'] ['id_mapping'] -%}
                    {%- if id_mapping | length > 0 %}
                        {%- set map_primary_key = [] -%}
                        {%- for id_map_definition in id_mapping %}
                            {%- if id_map_definition ['map_id'] == True %}
                                {%- set map_primary_key = map_primary_key.append(id_map_definition ['map_column']) -%}
                            {% endif -%}
                        {% endfor -%}
                    {% endif -%}
                    {%- set event_map_list = stream ['stream'] ['jsonSchema'] ['metadata'] ['event_map'] -%}
                    {%- if event_map_list | length > 0 %}
                        {%- for event_map in event_map_list %}
                           {%- do mapped_column.update({ event_map.get('map_event') :  event_map.get('map_column') }) -%}
                        {% endfor -%}
                        {%- for key1 in super_dict %}
                            {% if loop.first %}
                                UNION SELECT
                                {{ map_primary_key[0] }} as "user_id",
                            {% endif -%}
                            {%- if key1 in mapped_column %}
                               {{ mapped_column.get(key1) }} AS "{{ super_dict.get(key1) }}",
                            {% else %}
                               null AS "{{ super_dict.get(key1) }}",
                            {% endif -%}
                            {%- if loop.last %}
                            'sdk' AS "channel",
                            etl_run_datetime as  event_datetime
                            FROM {{ sourceName }}.hist_{{ table_name }} {% endif -%}
                        {% endfor -%}
                    {% endif -%}
                {% endfor -%}
            {% endfor -%}
        {% endif -%}
    {% endif -%}
{% endmacro %}