{{
    config(
        materialized='dummy_materialization',
        alias='DEPARTMENT',
        database='DBT_DB_DEV',
        schema='BRONZE'
    )
}}

{{ audit_logging_insert_macro() }}

{{ ingestion_csv_macro(model) }}

{{ file_archive_macro(model) }}

{{ audit_logging_update_macro() }}
