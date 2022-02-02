{% macro known_utm_and_headers_cols() %}
    {{ return(['headers_origin', 'search_utm_campaign', 'search_utm_content', 'search_utm_medium', 'search_utm_source', 'search_utm_term', 'search_gclid', 'search_fbclid', 'search_twclid']) }}
{% endmacro %}

{% macro google_ads_unified() %}
        {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
        {%- if source_relation != none %}
            {%- set get_active_pipelines %}
                SELECT
                     lower(replace(source_name, ' ', '_')) as source_name
                FROM
                    (SELECT
                         payload,
                         source_name,
                         source_type,
                         name,
                         created_at,
                         updated_at,
                         is_deleted,
                         row_number()
                        over (partition BY name
                    ORDER BY  updated_at DESC) AS RN
                    FROM {{ env_var('SCHEMA') }}.connection_pipeline) emap
                WHERE emap.RN = 1 AND is_deleted = 0 AND lower(source_type) LIKE '%google%ads%'
            {% endset -%}
            {%- set results = run_query(get_active_pipelines) -%}
            {%- if execute %}
                {%- set source_name_list = results.columns [0].values() -%}
            {% else %}
                {%- set source_name_list = [] -%}
            {% endif -%}

            {%- if source_name_list|length > 0 %}
                    {%- set schema_exists = [] -%}
                    {%- for source_name in source_name_list %}
                        {%- set source_schema_name = source_name + '_'  + env_var('ENV') -%}
                        {%- set check_relation = adapter.get_relation(
                             database = env_var('DATABASE'),
                             schema = source_schema_name,
                             identifier = "ad_group_ad_report")
                        -%}
                         {%- set check_click_view_relation = adapter.get_relation(
                              database = env_var('DATABASE'),
                              schema = source_schema_name,
                              identifier = "click_view")
                         -%}
                        {% if check_relation != None and check_click_view_relation != None %}
                             {%- set schema_exists = schema_exists.append(source_schema_name) -%}
                        {% endif -%}
                    {% endfor -%}
                     {%- if schema_exists|length > 0 %}
                        {% call statement() -%}
                            {%- for sch in schema_exists %}
                                {%- if loop.first %}
                                   CREATE TABLE
                                    {{ env_var('SCHEMA') }}.google_ads AS
                                    SELECT tl.*, tr."campaign_start_date", tr."campaign_end_date" FROM (
                                    SELECT l.*, r.google_ads_click_id FROM (SELECT
                                        DISTINCT *
                                    FROM (
                                {% endif -%}
                                SELECT
                                     "campaign.id" AS google_ads_campaign_id,
                                     "ad_group.name" AS google_ads_group_name,
                                     "campaign.name" AS google_ads_campaign_name,
                                     '{{ sch }}' AS google_ads_provider_type
                                 FROM
                                     "{{ sch }}"."ad_group_ad_report"
                                 {%- if not loop.last %} UNION {% else %} ))l {% endif -%}
                            {% endfor -%}

                            {%- for sch in schema_exists %}
                                {%- if loop.first %}
                                    JOIN (SELECT
                                        DISTINCT *
                                    FROM (
                                {% endif -%}
                                SELECT
                                     "click_view.gclid" AS google_ads_click_id,
                                     "campaign.id" AS google_ads_campaign_id
                                 FROM
                                     "{{ sch }}"."click_view"
                                 {%- if not loop.last %} UNION {% else %} ))r ON
                                    l.google_ads_campaign_id = r.google_ads_campaign_id) tl
                                 {% endif -%}
                            {% endfor -%}

                            {%- for sch in schema_exists %}
                                {%- if loop.first %}
                                    JOIN (SELECT
                                        *
                                    FROM (
                                {% endif -%}
                                SELECT
                                    "campaign.id",
                                    "campaign.start_date" AS "campaign_start_date",
                                    "campaign.end_date" AS "campaign_end_date"
                                 FROM
                                 "{{ sch }}"."campaigns"
                                 {%- if not loop.last %} UNION {% else %} ))tr ON
                                    tl.google_ads_campaign_id = tr."campaign.id"
                                 {% endif -%}
                            {% endfor -%}

                        {%- endcall %}
                    {% endif -%}
            {% endif -%}
        {% endif -%}
{% endmacro %}

{% macro camel_to_snake(s) -%}
    {%- set uppers = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' | list -%}
    {%- set output = [] %}
    {%- for char in (
            s | list
        ) -%}
        {%- if char in uppers -%}
            {% do output.append("_") %}
            {% do output.append(char.lower()) %}
        {%- else -%}
            {% do output.append(char) %}
        {%- endif %}
    {%- endfor -%}

    {{ return("".join(output) | replace('-', '_') ) }}
{%- endmacro %}

{% macro get_records_count_in_last_interval(database_name, schema_name, table_name, interval_in_hours) %}

    {%- set metadata_source_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=table_name) -%}
    {% if metadata_source_relation %}
        {%- set get_active_pipelines %}
            SELECT COUNT(*) FROM {{ env_var('SCHEMA') }}.{{ table_name }}
                WHERE event_datetime  > to_iso8601(CURRENT_TIMESTAMP - INTERVAL '{{ interval_in_hours }}' HOUR)
        {% endset -%}
        {%- set results = run_query(get_active_pipelines) -%}
        {{ log("dbt-blotout-utils:output " ~ results.columns[0].values()[0], info=True) }}
    {% else %}
        {{ log("dbt-blotout-utils:output -1", info=True) }}
    {% endif %}
{% endmacro %}