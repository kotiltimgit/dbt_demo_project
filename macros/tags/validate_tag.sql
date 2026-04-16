{% macro validate_tag(tag_name) %}

    {% set matched_models = [] %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' and tag_name in node.tags %}
            {% do matched_models.append(node.name) %}
        {% endif %}
    {% endfor %}

    {% if matched_models | length == 0 %}
        {% do exceptions.raise_compiler_error(
            "No models found for tag: " ~ tag_name
        ) %}
    {% else %}
        {{ log("Found models for tag: " ~ tag_name, info=True) }}
    {% endif %}

{% endmacro %}