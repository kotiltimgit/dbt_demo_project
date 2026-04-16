{{
    config(
        materialized='incremental',
        unique_key=['EMPLOYEE_ID','valid_from'],
        schema='BRONZE'
    )
}}

{{
    scd_type2_new(
        source_query=" SELECT * FROM "~source('EMPLOYEE_RAW', 'EMPLOYEE')~" ",
        natural_key='EMPLOYEE_ID',
        tracked_columns=['EMPLOYEE_LOCATION', 'EMPLOYEE_SALARY'],
        all_columns=['EMPLOYEE_ID', 'EMPLOYEE_NAME', 'EMPLOYEE_SALARY', 'EMPLOYEE_LOCATION', 'DEPT_ID']
    )
}}