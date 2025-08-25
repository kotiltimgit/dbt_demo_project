SELECT
    '{{ invocation_id }}' as invocation,
    '{{ env_var('DBT_CLOUD_PROJECT_ID', '') }}' as project_id,
    '{{ project_name }}' as project_name,
    '{{ env_var('DBT_CLOUD_ENVIRONMENT_ID', '') }}' as environment_id,
    '{{ env_var('DBT_CLOUD_ENVIRONMENT_NAME', '') }}' as environment_name,
    '{{ target.name }}' as environment_type,
    '{{ env_var('DBT_CLOUD_JOB_ID', '') }}' as job_id,
    '{{ env_var('DBT_CLOUD_JOB_NAME', '') }}' as job_name