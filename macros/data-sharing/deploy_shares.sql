{% macro deploy_shares() %}

{#
    This macro deploys the shares as defined in our dbt_project.yml config block.
    For example the following config block...

    shares:
    - name: share1
      databases:
        - PROD_DB
      accounts:
        - ACCOUNT123
    
    Would grant share access on PROD_DB to account ACCOUNT123. The specific grant logic and DDL commands 
    are contained in the macro grant_usage_on_shares.
#}


{% for share in var('shares') %} }}
    {% set sql %}
    {{ grant_usage_on_shares(share.name, share.databases, share.accounts, share.schemas, share.tables) }}
    {% endset %}
    {% if target.name == 'prod' %}
        {% do run_query(sql) %}
        {{ log("Granting access on database: " ~ database ~ " to share: " ~ share, info=True) }}
    {% else %}
        {% set sql %}
        select 1=1;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}

{% endfor %}
{% endmacro %}