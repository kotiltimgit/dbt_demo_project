-- depends_on: {{ ref('dbt_job_schedules') }}

{{
    config(
        materialized='view', 
        database='DBT_DB_DEV',
        schema='GOLD',
        alias='VW_JOB_SCHEDULES'
    )
}}

select 
* from {{ ref('job_schedules') }}
