{{
    config(
        materialized='view'
    )
}}

select * from {{ ref('department') }}