{{
    config(
        materialized='view',
        database='DBT_DB_DEV',
        schema='GOLD',
        alias='VW_JOB_SCHEDULES'
    )
}}

select * from {{ ref('job_schedules') }}
