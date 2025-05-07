{% macro objectset_snapshots(snapshots) %}
    
    {% if snapshots %}
        {% set snapshot_values %}
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
            $13,
            parse_json($14)
        from values
        {% for snapshot in snapshots -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ snapshot.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ snapshot.database }}', {# database #}
                '{{ snapshot.schema }}', {# schema #}
                '{{ snapshot.name }}', {# name #}
                '{{ tojson(snapshot.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ snapshot.package_name }}', {# package_name #}
                '{{ snapshot.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ snapshot.checksum.checksum | replace('\\', '\\\\') }}', {# checksum #}
                '{{ snapshot.config.strategy }}', {# strategy #}
                '{{ tojson(snapshot.config.meta) | replace("\\", "\\\\") | replace("'","\\'") | replace('"', '\\"') }}', {# meta #}
                '{{ snapshot.alias }}', {# alias #}
                {% if var('dbt_artifacts_exclude_all_results', false) %}
                    null
                {% else %}
                    '{{ tojson(snapshot) | replace("\\", "\\\\") | replace("'","\\'") | replace('"', '\\"') }}' {# all_results #}
                {% endif %}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ snapshot_values }}
    {% else %} {{ return("") }}
    {% endif %}

{% endmacro %}