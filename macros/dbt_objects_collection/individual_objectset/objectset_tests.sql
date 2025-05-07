{% macro objectset_tests(tests) %}
    
    {% if tests %}
        {% set test_values %}
        select
            $1,
            $2,
            $3,
            $4,
            parse_json($5),
            $6,
            $7,
            parse_json($8),
            parse_json($9)
        from values
        {% for test in tests -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ test.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ test.name }}', {# name #}
                '{{ tojson(test.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ test.package_name }}', {# package_name #}
                '{{ test.original_file_path | replace('\\', '\\\\') }}', {# test_path #}
                '{{ tojson(test.tags) }}', {# tags #}
                {% if var('dbt_artifacts_exclude_all_results', false) %}
                    null
                {% else %}
                    '{{ tojson(test) | replace("\\", "\\\\") | replace("'","\\'") | replace('"', '\\"') }}' {# all_fields #}
                {% endif %}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ test_values }}
    {% else %} {{ return("") }}
    {% endif %}

{% endmacro %}