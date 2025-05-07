{{
    config(
        materialized='table',
        alias='Test_Table_Unique',
        schema='PUBLIC'
    )
}}

select 101 as reg_no, 'syed' as name
union all
select 102 as reg_no, 'ahamed' as name
union all
select 101 as reg_no, 'kote' as name


{% set v = ['d', 's'] %}
{% set l = 'k' %}

{% if l in v %}
    {{ l }}
        
{% endif %}
{{project_name}}
{%- set l = ["model.dbt_demo_project.dbt_job_schedules", "model.dbt_demo_project.sources", "model.dbt_demo_project.exposures"] -%}
{{ graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("package_name", "equalto", project_name) | rejectattr("unique_id", "in", l) | list}}

{% set q_result = run_query("SELECT COMMAND_INVOCATION_ID FROM DBT_DB_DEV.DBT_PACKAGE_SCHEMA.INVOCATIONS") %}
{% if execute %}
    {{ q_result.columns['COMMAND_INVOCATION_ID'].values() | list }}
        
{% endif %}