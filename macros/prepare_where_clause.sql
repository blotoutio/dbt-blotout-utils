{% macro prepare_where_clause(column_name, operator_type, value_list) %}
    {% if value_list|length > 0 %}
        and ( lower(cast({{ column_name }} as varchar))
        {% if operator_type == 'STARTS_WITH' %}
            {% for vals in value_list %}
                LIKE (lower('{{ vals }}%'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'END_WITH' %}
            {% for vals in value_list %}
                LIKE (lower('%{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'CONTAINS' or operator_type == 'LIKE' %}
            {% for vals in value_list %}
                LIKE (lower('%{{ vals }}%'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'NOT_CONTAINS' or operator_type == 'NOT_LIKE' %}
            {% for vals in value_list %}
                NOT LIKE (lower('%{{ vals }}%'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'EQUAL' or operator_type == 'IN' %}
            {% for vals in value_list %}
                IN (lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'NOT_EQUAL' or operator_type == 'NOT_IN' %}
            {% for vals in value_list %}
                NOT IN (lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'GREATER_THAN' %}
            {% for vals in value_list %}
                > (lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'GREATER_THAN_OR_EQUAL_TO' %}
            {% for vals in value_list %}
                >= (lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'LESS_THAN' %}
            {% for vals in value_list %}
                <(lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% elif operator_type == 'LESS_THAN_OR_EQUAL_TO' %}
            {% for vals in value_list %}
                <= (lower('{{ vals }}'))
                {% if not loop.last %} OR lower(cast({{ column_name }} as varchar)) {% endif %}
            {% endfor %}
        {% endif %}
        )
    {% endif -%}
{% endmacro %}
