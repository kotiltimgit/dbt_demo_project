{{
    config(
        pre_hook="{{ audit_logging_prehook() }}"
    )
}}
{{ test_audit() }}