
{% macro drop_unused_tables(schema, dry_run=True) %}
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}

  {% set query %}
    with existing as (
      select lower(table_schema) as schema_name,
             lower(table_name) as ref_name,
             'table' as ref_type
      from information_schema.tables
      where lower(table_schema) IN (
        {%- if schema is iterable and (schema is not string and schema is not mapping) -%}
          {%- for s in schema -%}
            '{{ s.lower() }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema.lower() }}'
        {%- endif -%}
      )

      union all
      
      select lower(table_schema) as schema_name,
             lower(table_name) as ref_name,
             'view' as ref_type
      from information_schema.views
        where lower(table_schema) IN (
        {%- if schema is iterable and (schema is not string and schema is not mapping) -%}
          {%- for s in schema -%}
            '{{ s.lower() }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema.lower() }}'
        {%- endif -%}
        )
    )

    , desired as (
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %}
        select
        '{{ node.schema.lower() }}' as schema_name
         ,'{{node.name}}' as ref_name
        {% if not loop.last %} union all {% endif %}
      {%- endfor %}
    )

    , final as (
        select existing.schema_name,
               existing.ref_name,
               existing.ref_type
        from existing
        left join desired
            on existing.schema_name = desired.schema_name
            and existing.ref_name = desired.ref_name
        where desired.ref_name is null
    )

    select * from final
  {% endset %}
  {%- set result = run_query(query) -%}
  {% if result %}
      {%- for to_delete in result -%}
        {%- if dry_run -%}
            {%- do log('To be dropped: ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
        {%- else -%}
            {%- do log('Dropping ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
            {% set drop_command = 'DROP ' ~ to_delete[2] ~ ' IF EXISTS ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ ' CASCADE;' %}
            {% do run_query(drop_command) %}
            {%- do log('Dropped ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
        {%- endif -%}
      {%- endfor -%}
  {% else %}
    {% do log('No orphan tables to clean.', True) %}
  {% endif %}
{% endmacro %}