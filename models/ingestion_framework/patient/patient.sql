{{
    config(
        materialized='table',
        alias='PATIENT',
        database=env_var('DBT_ENV_DB'),
        schema='BRONZE',
        pre_hook=["{{ audit_logging_insert_macro() }}", "{{ ingestion_json_macro() }}"],
        post_hook=["{{ file_archive_macro() }}", "{{ audit_logging_update_macro() }}"]
    )
}}

SELECT 
--t2.value:address as address,
t2.value:address:city::string as city,
t2.value:address:country::string as country,
t2.value:address:state::string as state,
t2.value:blood_type::string as blood_type,
--t2.value:contact as contact,
t2.value:contact:email::string as email,
t2.value:contact:phone::string as phone,
t2.value:dob::date as dob,
--t2.value:emergency_contact as emergency_contact,
t2.value:emergency_contact:name::string as emergency_contact_name,
t2.value:emergency_contact:phone::string as emergency_contact_phone,
t2.value:emergency_contact:relation::string as emergency_contact_relation,
t2.value:gender::string as gender,
--t2.value:insurance as insurance,
t2.value:insurance:policy_no::string as insurance_policy_no,
t2.value:insurance:provider::string as insurance_provider,
t2.value:insurance:valid_till::date as insurance_valid_till,
--t2.value:name as name,
t2.value:name:first::string as first_name,
t2.value:name:full::string as full_name,
t2.value:name:last::string as last_name,
t2.value:patient_id::number as patient_id
FROM DBT_DB_DEV.BRONZE.RAW_PATIENT STG,
lateral flatten( input => STG.RAW_DATA:patients ) t2
