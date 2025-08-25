{% macro ingestion_partition_csv_files_macro(stage_relation='', stage_rel_path='') %}
    
    {%- set stage_full_path = stage_relation ~ '/' ~ stage_rel_path -%}
    {%- set query_result = run_query('LIST @' ~ stage_full_path) -%}
    {#{% if execute %}#}
    {{ log("QUERY RESULT ----> " ~ query_result, info=True) }}

    {% set query_result_list = [] %}
    {% for row in query_result.rows %}
        {% do query_result_list.append(dict(zip(query_result.column_names, row))) %}
    {% endfor %}
    {{ log("QUERY RESULT DICT ----> ", info=True) }}
    {{ log(query_result_list, info=True) }}
    {#{% endif %}#}
    
{% endmacro %}