{% macro known_utm_and_headers_cols() %}
    {{ return(['headers_origin', 'search_utm_campaign', 'search_utm_content', 'search_utm_medium', 'search_utm_source', 'search_utm_term', 'search_gclid', 'search_fbclid', 'search_twclid']) }}
{% endmacro %}