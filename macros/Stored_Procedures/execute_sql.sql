{% macro execute_sql(sql_stmt) %}

    {{ log("Executing SQL: " ~ sql_stmt, info=True) }}

    {% do run_query(sql_stmt) %}

{% endmacro %}