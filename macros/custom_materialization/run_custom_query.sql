{# a user-friendly interface into statements #}
{% macro run_custom_query(sql, auto_begin=false) %}
  {% call statement("custom_run_query_statement", fetch_result=true, auto_begin=auto_begin) %}
    {{ sql }}
  {% endcall %}

  {% do return(load_result("custom_run_query_statement")) %}

{% endmacro %}