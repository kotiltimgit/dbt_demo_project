{% macro objectset_models(models) %}

    {% if models %}
        {% set model_values %}
        select
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            parse_json($7),
            $8,
            $9,
            $10,
            $11,
            parse_json($12),
            parse_json($13),
            $14,
            parse_json($15),
            $16
        from values
        {% for model in models -%}
                {% set model_copy = model.copy() -%}
                {% do model_copy.pop('raw_code', None) %}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model_copy.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ model_copy.database }}', {# database #}
                '{{ model_copy.schema }}', {# schema #}
                '{{ model_copy.name }}', {# name #}
                '{{ tojson(model_copy.depends_on.nodes) | replace('\\', '\\\\') }}', {# depends_on_nodes #}
                '{{ model_copy.package_name }}', {# package_name #}
                '{{ model_copy.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ model_copy.checksum.checksum  | replace('\\', '\\\\') }}', {# checksum #}
                '{{ model_copy.config.materialized }}', {# materialization #}
                '{{ tojson(model_copy.tags) }}', {# tags #}
                '{{ tojson(model_copy.config.meta) | replace("\\", "\\\\") | replace("'","\\'") | replace('"', '\\"') }}', {# meta #}
                '{{ model_copy.alias }}', {# alias #}
                {% if var('dbt_artifacts_exclude_all_results', false) %}
                    null,
                {% else %}
                    '{{ tojson(model_copy) | replace("\\", "\\\\") | replace("'","\\'") | replace('"', '\\"') }}', {# all_results #}
                {% endif %}
                'Y' {# is_active #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %} {{ return("") }}
    {% endif %}

{% endmacro %}