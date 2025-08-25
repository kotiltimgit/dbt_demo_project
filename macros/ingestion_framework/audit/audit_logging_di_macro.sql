{% macro audit_logging_di_macro() %}
    {# Building Audit Table Relation #}
    {%- set database = var("audit_logging_database", target.database) -%}
    {%- set schema = var("audit_logging_schema", "LOGGING") -%}
    {%- set table_identifier = var("audit_logging_table_name", "DBT_RUN_LOG") -%}
    {%- set relation = database ~ '.' ~ schema ~ '.' ~ table_identifier -%}

    {# Building Delete Invocation From Audit Table Query #}
    {{ log("Deleting this Invocation's Log to Insert Complete Information from Results Context", info=True) }}
    {% call statement('invocation_result_delete_sql') %}
        DELETE FROM {{ relation }}
        WHERE INVOCATION_ID = '{{ invocation_id }}'
        ;
    {% endcall %}

    {# Building Insert Into Audit Table Query #}
    {%- set results_content_sql = get_audit_log_results_content_sql() -%}

    {{ log("Logging Completion Information from Results Context", info=True) }}
    {% call statement('invocation_result_insert_sql') %}
        INSERT INTO {{ relation }} (
            INVOCATION_ID,
            PROJECT_ID,
            PROJECT_NAME,
            ENVIRONMENT_ID,
            ENVIRONMENT_NAME,
            JOB_ID,
            JOB_NAME,
            OBJECT_UID,
            OBJECT_NAME,
            OBJECT_TYPE,
            {# OBJECT_STATUS, #}
            {# OBJECT_START_TIME, #}
            {# OBJECT_END_TIME, #}
            RUN_START_TIME,
            WAS_FULL_REFRESH,
            THREAD_ID,
            RUN_STATUS,
            COMPILE_START_TIME,
            RUN_END_TIME,
            DURATION,
            {# SOURCE_RECORD_COUNT, #}
            {# INSERT_RECORD_COUNT, #}
            {# UPDATE_RECORD_COUNT, #}
            {# ERROR_RECORD_COUNT, #}
            MATERIALIZATION,
            DATABASE_NAME,
            SCHEMA,
            ALIAS,
            ERROR_MESSAGE,
            ADAPTER_RESPONSE
        )
        {{ results_content_sql }}
        ;
    {% endcall %}

{% endmacro %}

{% macro get_audit_log_results_content_sql() %}
    {%- set supported_resource_types = ['model', 'seed', 'snapshot'] -%}

    {# By Using Context Variables, Environment Variables, and Snowflake Function #}
    {# Building Select Query for Audit DI Macro #}
    {% set select_sql %}
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22,
        PARSE_JSON($23)
    FROM VALUES

    {% for node_object in results | selectattr("node.resource_type", "in", supported_resource_types) | list %}
        (
            '{{ invocation_id }}', {# 1. command_invocation_id -- Context Variable #}
            '{{ env_var('DBT_CLOUD_PROJECT_ID', '') }}', {# 2. dbt_cloud_project_id #}
            '{{ project_name }}',
            '{{ env_var('DBT_CLOUD_ENVIRONMENT_ID', '') }}',
            '{{ target.name }}',
            '{{ env_var('DBT_CLOUD_JOB_ID', '') }}',
            '{{ env_var('DBT_CLOUD_JOB_NAME', '') }}',
            '{{ node_object.node.unique_id }}', {# 8. node_id #}
            '{{ node_object.node.name }}',
            '{{ node_object.node.resource_type }}',
            {# OBJECT_STATUS, #}
            {# OBJECT_START_TIME, #}
            {# OBJECT_END_TIME, #}
            '{{ run_started_at }}', {# 14. run_started_at -- Context Variable #}

            {% set config_full_refresh = node_object.node.config.full_refresh %}
            {% if config_full_refresh is none %}
                {% set config_full_refresh = flags.FULL_REFRESH %}
            {% endif %}
            '{{ config_full_refresh }}', {# 15. was_full_refresh #}

            '{{ node_object.thread_id }}', {# 16. thread_id #}
            '{{ node_object.status | upper }}', {# 17. status #}
            
            {% set compile_started_at = (node_object.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
            {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# 18. compile_started_at #}
            
            {% set query_completed_at = (node_object.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
            {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# 19. query_completed_at #}

            {{ node_object.execution_time }}, {# 20. total_node_runtime #}
            {# SOURCE_RECORD_COUNT, #}
            {# INSERT_RECORD_COUNT, #}
            {# UPDATE_RECORD_COUNT, #}
            {# ERROR_RECORD_COUNT, #}
            '{{ node_object.node.config.materialized }}',
            '{{ node_object.node.config.database }}',
            '{{ node_object.node.config.schema }}',
            '{{ node_object.node.config.alias }}',
            '{{ node_object.message | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}', {# 29. message #}
            '{{ tojson(node_object.adapter_response) | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}' {# 30. adapter_response #}
        )
        {%- if not loop.last %},{%- endif %}

    {% endfor %}
    {% endset %}

    {{ select_sql }}

{% endmacro %}