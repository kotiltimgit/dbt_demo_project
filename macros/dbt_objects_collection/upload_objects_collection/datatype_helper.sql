{# Providing some additional 'Snowflake' supported datatypes which is not in dbt internally for model creation #}

{% macro type_array() %}
    array
{% endmacro %}

{% macro type_bigint() %}
    bigint
{% endmacro %}

{% macro type_boolean() %}
    boolean
{% endmacro %}

{% macro type_float() %}
    float
{% endmacro %}

{% macro type_int() %}
    integer
{% endmacro %}

{% macro type_json() %}
    object
{% endmacro %}

{% macro type_numeric() %}
    numeric(28,6)
{% endmacro %}

{% macro type_string() %}
    varchar
{% endmacro %}

{% macro type_timestamp() %}
    timestamp
{% endmacro %}