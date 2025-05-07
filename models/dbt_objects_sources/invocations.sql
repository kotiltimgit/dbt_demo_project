select * from (
    select
        cast(null as {{ dbt_demo_project.type_string() }}) as command_invocation_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_version,
        cast(null as {{ dbt_demo_project.type_string() }}) as project_name,
        cast(null as {{ dbt_demo_project.type_timestamp() }}) as run_started_at,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_command,
        cast(null as {{ dbt_demo_project.type_boolean() }}) as full_refresh_flag,
        cast(null as {{ dbt_demo_project.type_string() }}) as target_profile_name,
        cast(null as {{ dbt_demo_project.type_string() }}) as target_name,
        cast(null as {{ dbt_demo_project.type_string() }}) as target_schema,
        cast(null as {{ dbt_demo_project.type_int() }}) as target_threads,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_cloud_project_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_cloud_job_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_cloud_run_id,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_cloud_run_reason_category,
        cast(null as {{ dbt_demo_project.type_string() }}) as dbt_cloud_run_reason,
        cast(null as {{ dbt_demo_project.type_json() }}) as env_vars,
        cast(null as {{ dbt_demo_project.type_json() }}) as dbt_vars,
        cast(null as {{ dbt_demo_project.type_json() }}) as invocation_args,
        cast(null as {{ dbt_demo_project.type_json() }}) as dbt_custom_envs
) as empty_table
where 1 = 0