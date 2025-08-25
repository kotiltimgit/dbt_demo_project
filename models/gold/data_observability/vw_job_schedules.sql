-- depends_on: {{ ref('dbt_job_schedules') }}

{{
    config(
        materialized='view', 
        database=env_var('DBT_ENV_DB'),
        schema='GOLD',
        alias='VW_JOB_SCHEDULES'
    )
}}

select 
* from {{ ref('job_schedules') }}
