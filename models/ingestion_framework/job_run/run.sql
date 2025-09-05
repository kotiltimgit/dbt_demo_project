{{
    config(
        materialized='dummy_materialization',
        alias='RUN',
        database=env_var('DBT_ENV_DB'),
        schema='BRONZE'
    )
}}

{{ audit_logging_insert_macro() }}

{{ ingestion_json_macro() }}

{#{{ file_archive_macro() }}#}

{{ audit_logging_update_macro() }}