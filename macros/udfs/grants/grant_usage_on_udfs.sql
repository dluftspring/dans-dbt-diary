
{% macro grant_usage_on_udfs() %}

{% set udf_meta_query %}
    show user functions;
{% endset %}

{% set collect_udf_meta_query %}
    select "name" as udf_name
        , trim(split("arguments",'RETURN')[0]::string) as udf_signature
    from table(result_scan(last_query_id()));
{% endset %}

{% do run_query(udf_meta_query) %}

{% set results = run_query(collect_udf_meta_query) %}

{% if execute %}
    {% set results_list = results.rows.values() %}
{% else %}
    {% set results_list = [] %}
{% endif %}


--Now we can run the grant statements properly
{% if target.name == 'prod' %}
    {% for udf,sig in results_list %}
        {% for role in var('prod_roles') %}
            grant usage on function {{ sig }} to role {{ role }};
            {{ log("Granting usage on function: " ~ udf ~ " to role " ~ role, info=True) }}
        {% endfor %}
    {% endfor %}
{% elif target.name == 'dev' %}
    {% for udf,sig in results_list %}
        {% for role in var('prod_roles') %}
            grant usage on function {{ sig }} to role {{ role }};
            {{ log("Granting usage on function: " ~ udf ~ " to role " ~ role, info=True) }}
        {% endfor %}
    {% endfor %}
{% else %}
    select 1;
{% endif %}

{% endmacro %}