{% macro audit_logging_update_macro() %}
    {# Building Audit Table Relation #}
    {%- set database = var("audit_logging_database", target.database) -%}
    {%- set schema = var("audit_logging_schema", "LOGGING") -%}
    {%- set table_identifier = var("audit_logging_table_name", "DBT_RUN_LOG") -%}
    {%- set relation = database ~ '.' ~ schema ~ '.' ~ table_identifier -%}

    {# Building Update Audit Table Query #}
    {%- set update_content_sql = get_audit_log_update_content_sql(model) -%}

    {{ log("Logging Completion Information for Node '" ~ model.unique_id ~ "'", info=True) }}
    {% call statement('node_complete_info_update_sql') %}
        UPDATE {{ relation }} target
        SET
        target.OBJECT_STATUS = input.OBJECT_STATUS,
        target.OBJECT_END_TIME = input.OBJECT_END_TIME
        FROM (
            {{ update_content_sql }}
        ) input
        WHERE
        target.INVOCATION_ID = input.INVOCATION_ID
        AND
        target.OBJECT_UID = input.OBJECT_UID
        ;
    {% endcall %}
    
{% endmacro %}

{% macro get_audit_log_update_content_sql(model) %}
    {# By Using Context Variables, and Snowflake Function #}
    {# Building Select Query for Audit Update Macro #}
    SELECT
    '{{ invocation_id }}' AS INVOCATION_ID,
    '{{ model.unique_id }}' AS OBJECT_UID,
    'COMPLETED' AS OBJECT_STATUS,
    CURRENT_TIMESTAMP() AS OBJECT_END_TIME

{% endmacro %}