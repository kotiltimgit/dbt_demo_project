{% macro ingestion_partition_csv_files_macro() %}
    {% if execute %}
    {% set pattern = model.meta.source_location_conf.pattern %}

    {% if pattern %}

    {# Building Audit Table Relation #}
    {%- set database = var("audit_logging_database", target.database) -%}
    {%- set schema = var("audit_logging_schema", "LOGGING") -%}
    {%- set table_identifier = var("audit_logging_table_name", "DBT_RUN_LOG") -%}
    {%- set audit_table_relation = database ~ '.' ~ schema ~ '.' ~ table_identifier -%}

    {%- set last_success_run_query -%}
        SELECT TO_VARCHAR(MAX(RUN_START_TIME), 'YYYYMMDDHH24MISS')
        FROM {{ audit_table_relation }}
        WHERE
        OBJECT_UID = '{{ model.unique_id }}' AND RUN_STATUS = 'SUCCESS'
        ;
    {% endset %}
    {%- set last_success_run_query_result = run_query(last_success_run_query) -%}
    {%- set last_success_run_timestamp = last_success_run_query_result.columns[0].values()[0] -%}
    {#{{ log("SUCCESS QUERY RESULT ----> " ~ last_success_run_timestamp, info=True) }}#}
    
    {%- set stage_name = model.meta.source_location_conf.stage_name -%}
    {%- set stage_landing_path = model.meta.source_location_conf.stage_landing_path -%}
    {%- set stage_full_path = stage_name ~ '/' ~ stage_landing_path -%}
    {%- set list_files_query -%}
        LIST @{{ stage_full_path }}/ PATTERN='{{ pattern }}' 
        ;
    {% endset %}
    {%- set list_files_query_result = run_query(list_files_query) -%}
    {#{% if execute %}
    {{ log("QUERY RESULT ----> " ~ list_files_query_result, info=True) }}#}

    {% set files_list = [] %}
    {% for row in list_files_query_result.rows %}
        {%- set file_last_modified_timestamp_rfc_type = modules.datetime.datetime.strptime(row.values()[3], "%a, %d %b %Y %H:%M:%S GMT") -%}
        {%- set file_last_modified_timestamp = file_last_modified_timestamp_rfc_type.strftime("%Y%m%d%H%M%S") -%}
        {#{{ log("File Timestamp --> " ~ file_last_modified_timestamp ~ "    :    Run Timestamp --> " ~ last_success_run_timestamp, info=True) }}#}
        {% if file_last_modified_timestamp > last_success_run_timestamp %}
            {% do files_list.append(row.values()[0].split('/')[-1]) %}
        {% endif %}
    {% endfor %}
    {{ log(files_list, info=True) }}

    {% endif %}
    {% endif %}
    
{% endmacro %}