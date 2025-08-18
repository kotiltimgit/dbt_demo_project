{% macro audit_logging(database=target.database, schema=target.schema, table_identifier='TEST_EXECUTIONS_RESULT') %}
    --select {{ invocation_id }}
    {%- set relation_exists = adapter.get_relation(database=database, schema=schema, identifier=table_identifier) -%}
    {%- set relation = database ~ '.' ~ schema ~ '.' ~ table_identifier -%}

    {% if not relation_exists %}
        {{ log("Audit Table Not Exist, So Executing 'CREATE TABLE SQL'", info=True) }}
        {% call statement('audit_create_table') %}
            create table {{ relation }} (
                TEST_EXECUTION_ID VARCHAR(2000) NOT NULL DEFAULT UUID_STRING(),
                COMMAND_INVOCATION_ID VARCHAR(2000),
                NODE_ID VARCHAR(2000),
                RUN_STARTED_AT TIMESTAMP_NTZ,
                THREAD_ID VARCHAR(2000),
                STATUS VARCHAR,
                COMPILED_CODE VARCHAR,
                COMPILE_STARTED_AT TIMESTAMP_NTZ,
                QUERY_COMPLETED_AT TIMESTAMP_NTZ,
                TOTAL_NODE_RUNTIME FLOAT,
                FAILURES INT,
                FAILURE_RECORDS VARIANT,
                MESSAGE VARCHAR
            )
            ;
        {% endcall %}
        {{ log("Target Table '" ~ relation ~ "' Created Successfully", info=True) }}
    
    {% else %}
        {{ log("Target Table '" ~ relation ~ "' Exist", info=True) }}
    {% endif %}
    
{% endmacro %}

{% macro get_audit_columns_content_sql(node_executions_list) %}
    {% set select_sql %}
    select
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
        $23,
        $24,
        $25,
        $26,
        parse_json($27)
    from values
    {% if node_executions_list %}
    {% for node_object in node_executions_list %}
        (
            '{{ invocation_id }}', {# 1. command_invocation_id -- Context Variable #}
            '{{ node_object.node.unique_id }}', {# 2. node_id #}
            '{{ run_started_at }}', {# 3. run_started_at -- Context Variable #}

            '{{ node_object.thread_id }}', {# 4. thread_id #}
            '{{ node_object.status }}', {# 5. status #}
            '{{ node_object.node.compiled_code}}', {# 6. compiled_query #}

            {% set compile_started_at = (node_object.timing | selectattr("name", "eq", "compile") | first | default({}))["started_at"] %}
            {% if compile_started_at %}'{{ compile_started_at }}'{% else %}null{% endif %}, {# 7. compile_started_at #}

            {% set query_completed_at = (node_object.timing | selectattr("name", "eq", "execute") | first | default({}))["completed_at"] %}
            {% if query_completed_at %}'{{ query_completed_at }}'{% else %}null{% endif %}, {# 8. query_completed_at #}

            {{ node_object.execution_time }}, {# 9. total_node_runtime #}
            {{ 'null' if node_object.failures is none else node_object.failures }}, {# 10. failures #}
            
            {% set failure_records = [] %}
            {% if node_object.failures != 0 %}
                {% set failure_records_table = run_query(node_object.node.compiled_code)  %}
                {% for row in failure_records_table.rows %}
                    {% set row_records = {} %}
                    {% for col_name, col_value in row.items() %}
                        {% do row_records.update({col_name: col_value}) %}
                    {% endfor %}
                    {% do failure_records.append(row_records) %}
                {% endfor %}
            {% endif %}
            '{{ failure_records | tojson }}', {# 11. failure_records #}

            '{{ node_object.message | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}' {# 12. message #}
        )
        {%- if not loop.last %},{%- endif %}
    {% endfor %}

    {% else %}

    {% endif %}
    {% endset %}

    {{ select_sql }}

{% endmacro %}