{{
    config(
        materialized='table',
        alias='Test_Table_Unique',
        schema='TESTING'
    )
}}

select 101 as reg_no, 'syed' as name
union all
select 102 as reg_no, 'ahamed' as name
union all
select 101 as reg_no, 'kote' as name

