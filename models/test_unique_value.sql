/*{{
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
select 101 as reg_no, 'kote' as name*/

{% set src_tbl_fqn = "DBT_DB_DEV.TESTING.STG_EMPLOYEES" %}
{%- set src_relation = adapter.get_relation(
        database=src_tbl_fqn.split('.')[0],
        schema=src_tbl_fqn.split('.')[1],
        identifier=src_tbl_fqn.split('.')[2]
) %}

{% set tgt_tbl_fqn = "DBT_DB_DEV.TESTING.EMPLOYEES" %}
{%- set tgt_relation = adapter.get_relation(
        database=tgt_tbl_fqn.split('.')[0],
        schema=tgt_tbl_fqn.split('.')[1],
        identifier=tgt_tbl_fqn.split('.')[2]
) %}

{% set pk_cols = ['EMP_ID'] %}

{{ compare_records(src_relation, tgt_relation, pk_cols, 'indicator') }}

 

