{% macro known_utm_and_headers_cols() %}
    {{ return(['headers_origin', 'search_utm_campaign', 'search_utm_content', 'search_utm_medium', 'search_utm_source', 'search_utm_term', 'search_gclid', 'search_fbclid', 'search_twclid']) }}
{% endmacro %}

{% macro google_ads_unified() %}
    {% call statement() -%}
        {%- set source_relation = adapter.get_relation(database = env_var('DATABASE'), schema = env_var('SCHEMA'), identifier = 'connection_pipeline') -%}
        {%- if source_relation != none %}
            {%- set get_active_pipelines %}
            SELECT
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
            WHERE emap.RN = 1 AND is_deleted = 0 AND source_name LIKE '%Google%'
            {% endset -%}
            {%- set results = run_query(get_active_pipelines) -%}
            {%- if execute %}
                {%- set source_name_list = results.columns [0].values() -%}
            {% else %}
                {%- set source_name_list = [] -%}
            {% endif -%}
            {%- set schema_exists = [] -%}
            {%- for source_name in source_name_list %}
                {%- set source_schema_name = source_name -%}

                {%- set check_relation = adapter.get_relation(
                     database = env_var('DATABASE'),
                     schema = source_schema_name,
                     identifier = "ad_group_ad_report")
                -%}
                {% if check_relation != None %}
                     {%- set schema_exists = schema_exists.append(source_schema_name) -%}
                {% endif -%}
            {% endfor -%}
            {%- for sch in schema_exists %}
                {%- if loop.first %}
                   CREATE TABLE
                    {{ env_var('SCHEMA') }}.google_ads AS
                    SELECT l.*, r.google_ads_click_id FROM (SELECT
                        DISTINCT *
                    FROM (
                {% endif -%}
                SELECT
                     "campaign.id" as google_ads_campaign_id,
                     "ad_group.name" as google_ads_group_name,
                     "campaign.name" as google_ads_campaign_name,
                     '{{ sch }}' as google_ads_provider_type
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
                     "click_view.gclid" as google_ads_click_id,
                     "campaign.id" as google_ads_campaign_id
                 FROM
                     "{{ sch }}"."click_view"
                 {%- if not loop.last %} UNION {% else %} ))r ON
                 l.google_ads_campaign_id = r.google_ads_campaign_id {% endif -%}
            {% endfor -%}
        {% endif -%}
    {%- endcall %}
{% endmacro %}