{% materialization dummy_materialization, adapter='default' -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') %}
        select 1
    {% endcall %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [this]}) }}

{% endmaterialization %}