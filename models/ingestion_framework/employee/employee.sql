{{
    config(
        materialized='dummy_materialization',
        alias='EMPLOYEE',
        database=env_var('DBT_ENV_DB'),
        schema='BRONZE'
    )
}}

{{ audit_logging_insert_macro() }}

{{ ingestion_csv_macro() }}

{{ file_archive_macro() }}

{{ audit_logging_update_macro() }}
