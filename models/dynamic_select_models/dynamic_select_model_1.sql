{{
    config(
        pre_hook='{{ test_audit() }}'
    )
}}
select 1 as id