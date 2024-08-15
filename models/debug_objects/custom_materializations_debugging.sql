{{
    config(
        materialized='copy_into_materialization',
        alias='MATILLION_SCHEDULES',
        database='DBT_PROD',
        schema='DEVELOPMENT_PUBLIC',
        external_stage='REST_API_INTEGRATION.API_INTEGRATION.AWS_STAGE',
        stage_file_path='DBT_METADATA/CSV/List Users.csv',
        file_format='REST_API_INTEGRATION.API_INTEGRATION.CSV_FORMAT'
    )
}}

----------------------------------------------------------------------------

{%- set A = [1, 2] -%}
{%- set B = ['x', 'y', 'z'] -%}
{%- set AB_cartesian = modules.itertools.product(A, B) -%}

{%- for item in AB_cartesian %}
  {{ item }}
{%- endfor -%}