select * from (
    select
        cast(null as {{ dbt_demo_project.type_string() }}) as command_invocation_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as node_id,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as run_started_at,
        cast(null as {{ dbt_demo_project.type_boolean() }}) as was_full_refresh,
        cast(null as {{ dbt_demo_project.type_string() }}) as thread_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as status,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as compile_started_at,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as query_completed_at,
        cast(null as {{ dbt_demo_project.type_float() }}) as total_node_runtime,
        cast(null as {{ dbt_demo_project.type_int() }}) as rows_affected,
        cast(null as {{ dbt_demo_project.type_int() }}) as failures,
        cast(null as {{ dbt_demo_project.type_string() }}) as message,
        cast(null as {{ dbt_demo_project.type_json() }}) as adapter_response
) as empty_table
where 1 = 0