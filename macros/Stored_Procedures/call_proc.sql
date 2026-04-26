{% macro call_proc(proc_name, args=[]) %}

{% set final_args = [] %}

{% for arg in args %}
    {% if arg is number %}
        {% do final_args.append(arg) %}
    {% elif arg is none %}
        {% do final_args.append("NULL") %}
    {% else %}
        {% do final_args.append("'" ~ arg ~ "'") %}
    {% endif %}
{% endfor %}

{% set sql %}
call {{ proc_name }}({{ final_args | join(", ") }});
{% endset %}

{{ log(sql, info=True) }}

{% do run_query(sql) %}

{% endmacro %}