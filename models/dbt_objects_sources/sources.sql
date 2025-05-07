select * from (
    select
        cast(null as {{ dbt_demo_project.type_string() }}) as command_invocation_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as node_id,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as run_started_at,
        cast(null as {{ dbt_demo_project.type_string() }}) as database,
        cast(null as {{ dbt_demo_project.type_string() }}) as schema,
        cast(null as {{ dbt_demo_project.type_string() }}) as source_name,
        cast(null as {{ dbt_demo_project.type_string() }}) as loader,
        cast(null as {{ dbt_demo_project.type_string() }}) as name,
        cast(null as {{ dbt_demo_project.type_string() }}) as identifier,
        cast(null as {{ dbt_demo_project.type_string() }}) as loaded_at_field,
        cast(null as {{ dbt_demo_project.type_array() }}) as freshness,
        cast(null as {{ dbt_demo_project.type_json() }}) as all_results
) as empty_table
where 1 = 0