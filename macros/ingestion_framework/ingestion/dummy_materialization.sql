{% materialization dummy_materialization, adapter='default' -%}

    {% call statement('main') %}
        select 1
    {% endcall %}

    {{ return({'relations': [this]}) }}

{% endmaterialization %}