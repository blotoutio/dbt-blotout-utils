{% macro get_meta_data(source_name, table_name, metadata_source_name, metadata_table_name, connection_id, key_name,bucket_layer) %}

{%- set source_relation = source(source_name, table_name) -%}
{%- set metadata_source_relation = adapter.get_relation(database= env_var('DATABASE'), schema= metadata_source_name, identifier=metadata_table_name) -%}


    {% set getMetadataMapping %}
         SELECT payload FROM
            (SELECT
                 payload,
                 name,
                 created_at,
                 updated_at,
                 is_deleted,
                 row_number()
                over (partition BY name
            ORDER BY  updated_at DESC) AS RN
            FROM {{ env_var('METADATA_SOURCE_NAME') }}.connection_pipeline WHERE name='{{ connection_id }}') emap
        WHERE emap.RN = 1 AND is_deleted = 0
    {% endset %}

    {% set metadata_results = run_query(getMetadataMapping) %}
   
    {% if execute %}
        {% set payloadList = metadata_results.columns[0].values() %}
    {% else %}
        {% set payloadList = [] %}
    {% endif %}

    {% if payloadList|length > 0 %}
        {% for payload in payloadList %}
                    {% set payloadObj = fromjson(payload) %}
                    {% set streams = payloadObj['syncCatalog']['streams'] %}
                
                    {% for stream in streams %}
                        {% set streamName = bucket_layer+"_"+stream['stream']['name'] %}
                        {% if streamName == table_name %}
                            {% set streamPayload = stream %}
                            {% set metaData = streamPayload['config'][key_name] %}
                            {{ return(metaData) }}    
                        {% endif %}
                    {% endfor %}    
        {% endfor %}
    {% endif %}
{% endmacro %}
