{% macro get_meta_data(source_name, table_name, metadata_source_name, metadata_table_name, connection_id, keyName ) %}

{%- set source_relation = source(source_name, table_name) -%}
{%- set metadata_source_relation = adapter.get_relation(database= env_var('DATABASE'), schema= metadata_source_name, identifier=metadata_table_name) -%}


    {% set getMetadataMapping %}
            select payload from {{metadata_source_name}}.{{metadata_table_name}} where name = '{{connection_id}}'
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
                        {% set streamName = "stg_"+stream['stream']['name'] %}
                        {% if streamName == table_name %}
                            {% set streamPayload = stream %}
                            {% set metaData = streamPayload['config'][keyName] %}
                            {{ return(metaData) }}    
                        {% endif %}
                    {% endfor %}    
        {% endfor %}
    {% endif %}
{% endmacro %}
