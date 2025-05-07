{% macro objectset_sources(sources) %}
    
    {% if sources %}
        {% set source_values %}
        select
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            parse_json($11),
            parse_json($12)
        from values
        {% for source in sources -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ source.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ source.database }}', {# database #}
                '{{ source.schema }}', {# schema #}
                '{{ source.source_name }}', {# source_name #}
                '{{ source.loader }}', {# loader #}
                '{{ source.name }}', {# name #}
                '{{ source.identifier }}', {# identifier #}
                '{{ source.loaded_at_field | replace("'","\\'") }}', {# loaded_at_field #}
                '{{ tojson(source.freshness) | replace("'","\\'") }}', {# freshness #}
                {% if var('dbt_artifacts_exclude_all_results', false) %}
                    null
                {% else %}
                    '{{ tojson(source) | replace("\\", "\\\\") | replace("'", "\\'") | replace('"', '\\"') }}' {# all_results #}
                {% endif %}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ source_values }}
    {% else %} {{ return("") }}
    {% endif %}

{% endmacro %}