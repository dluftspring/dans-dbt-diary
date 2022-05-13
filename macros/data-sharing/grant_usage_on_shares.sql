
{% macro grant_usage_on_shares(share, databases, accounts, schemas=None, tables=None) %}

{#
    This macro grants share access to the snowflake account numbers to all shares defined in the dbt_project.yml shares list

    Arguments:
        share: str
            The name of the share to be granted to the associated accounts
        databases: List[str]
            The database(s) to grant access to
        accounts: str or List[str]
            List of snowflake account numbers to grant access to
        schemas: List[str] (optional)
            List of schemas to grant access to
        tables: List[str] (optional)
            List of tables to grant access to
#}

{% if target.name == 'prod' %}
    create or replace share {{ share }};
    {% for db in databases %}
        grant usage on database {{ db }} to share {{ share }};
        {% if schemas %}
            {% for schema in schemas %}
                grant usage on schema {{ db }}.{{ schema }} to share {{ share }};
                {% if tables %}
                    {% for table in tables %}
                        grant select on table {{ db }}.{{ schema }}.{{ table }} to share {{ share }};
                    {% endfor %}
                {% else %}
                    grant select on all tables in schema {{ db }}.{{ schema }} to share {{ share }};
                {% endif %}
            {% endfor %}
        {% else %}
            grant usage on all schemas in database {{ db }} to share {{ share }};
            grant select on all tables in database {{ db }} to share {{ share }};
        {% endif %}
    {# This might have to change when we have more than a single prod schema #}
    {% endfor %}
    {% if accounts is string %}
        alter share {{ share }} add accounts={{ accounts }};
    {% else %}
        alter share {{ share }} add accounts={{ accounts|join(',') }};
    {% endif %}
{% else %}
    {{ log('WARNING: Unable to grant access to shares in the dev environment this macro should only be run in production', info=True) }}
{%endif%}

{% endmacro %}