{% macro dynamic_region_select(base_table, region) %}

    {% set db_name = target.database %}
    {% set schema_name = 'BRONZE' %}
    {% set full_table = base_table ~ '_' ~ region %}

    {% set column_query %}
        select column_name
        from {{ db_name }}.information_schema.columns
        where table_schema = upper('{{ schema_name }}')
          and table_name = upper('{{ full_table }}')
        order by ordinal_position
    {% endset %}

    {% set results = run_query(column_query) %}

    {% if execute %}
        {% set cols = results.columns[0].values() %}
    {% else %}
        {% set cols = [] %}
    {% endif %}

    select

    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}

    from {{ db_name }}.{{ schema_name }}.{{ full_table }}

    where REGION = '{{ region }}'

{% endmacro %}