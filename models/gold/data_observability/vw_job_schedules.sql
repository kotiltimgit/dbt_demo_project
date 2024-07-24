-- depends_on: {{ ref('job_schedules') }}
-- depends_on: {{ ref('dbt_schedules_test') }}

{{
    config(
        materialized='view',
        database='DBT_DB_DEV',
        schema='GOLD',
        alias='VW_JOB_SCHEDULES'
    )
}}

{% if config.get('platform_name') == 'MATILLION' %}
    -- This view is created by 'MATILLION_JOB_SCHEDULES'
    select * from {{ ref('job_schedules') }}

{% elif config.get('platform_name') == 'DBT' %}
    -- This view is created by 'DBT_JOB_SCHEDULES'
    select * from {{ ref('dbt_schedules_test') }}

{% endif %}

/*select * from config.get('database') ~ '.' ~ config.get('schema') ~ '.' ~ var()*/
