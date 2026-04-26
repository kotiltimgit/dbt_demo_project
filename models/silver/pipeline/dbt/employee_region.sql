{% set region = var('region') %}

{{
    config(
        materialized='view',
        schema='GOLD',
        alias='EMPLOYEE_' ~ region
    )
}}

select *
from {{ source('TEST', 'STG_EMPLOYEE_' ~ region) }}
where REGION = '{{ region }}'