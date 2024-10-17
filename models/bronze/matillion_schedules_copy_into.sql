/*

-------------------------------------------------
Properties needs to be defined in config() block
-------------------------------------------------

1. materialized -- '<string>'   (Required)
2. alias -- '<string>'   (Required)
3. database -- '<string>'   (Required)
4. schema -- '<string>'   (Required) 
5. external_stage -- '<string>'   (Required)
6. location_path -- '<string>'   (Required)
7. files -- ['<file_name_1>', '<file_name_2>', ...]   (Optional)
8. pattern -- '<string>'   (Optional)
9. file_format -- '<string>'   (Required)
10. copy_options -- {'on_error': '<value>', 'match_by_column_name': '<value>', .....}   (Optional)
11. validation_mode -- '<string>'   (Optional)

*/

{{
    config(
        materialized='copy_into_materialization',
        alias='MATILLION_SCHEDULES',
        database='DBT_DB_DEV',
        schema='BRONZE',
        external_stage='DBT_DB_DEV.BRONZE.AWS_STAGE',
        location_path='MATILLION_METADATA/CSV/Schedule API Endpoints.csv',
        file_format='DBT_DB_DEV.BRONZE.CSV_FORMAT',
        copy_options={'on_error': 'CONTINUE', 'match_by_column_name': 'CASE_INSENSITIVE'}
    )
}}

