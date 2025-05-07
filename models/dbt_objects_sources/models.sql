select * from (
    select
        cast(null as {{ dbt_demo_project.type_string() }}) as command_invocation_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as node_id,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as run_started_at,
        cast(null as {{ dbt_demo_project.type_string() }}) as database,
        cast(null as {{ dbt_demo_project.type_string() }}) as schema,
        cast(null as {{ dbt_demo_project.type_string() }}) as name,
        cast(null as {{ dbt_demo_project.type_array() }}) as depends_on_nodes,
        cast(null as {{ dbt_demo_project.type_string() }}) as package_name,
        cast(null as {{ dbt_demo_project.type_string() }}) as path,
        cast(null as {{ dbt_demo_project.type_string() }}) as checksum,
        cast(null as {{ dbt_demo_project.type_string() }}) as materialization,
        cast(null as {{ dbt_demo_project.type_array() }}) as tags,
        cast(null as {{ dbt_demo_project.type_json() }}) as meta,
        cast(null as {{ dbt_demo_project.type_string() }}) as alias,
        cast(null as {{ dbt_demo_project.type_json() }}) as all_results,
        cast(null as {{ dbt_demo_project.type_string() }}) as is_active
) as empty_table
where 1 = 0