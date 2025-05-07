{% macro get_objectset_table_content_values(objectset, objects_to_upload) %}

    {# Convert the results to data to be imported #}

    {% if objectset == 'model_executions' %}
        {% set content_values = dbt_demo_project.objectset_model_executions(objects_to_upload) %}
    {% elif objectset == 'seed_executions' %}
        {% set content_values = dbt_demo_project.objectset_seed_executions(objects_to_upload) %}
    {% elif objectset == 'test_executions' %}
        {% set content_values = dbt_demo_project.objectset_test_executions(objects_to_upload) %}
    {% elif objectset == 'snapshot_executions' %}
        {% set content_values = dbt_demo_project.objectset_snapshot_executions(objects_to_upload) %}
    {% elif objectset == 'exposures' %}
        {% set content_values = dbt_demo_project.objectset_exposures(objects_to_upload) %}
    {% elif objectset == 'models' %}
        {% set content_values = dbt_demo_project.objectset_models(objects_to_upload) %}
    {% elif objectset == 'seeds' %}
        {% set content_values = dbt_demo_project.objectset_seeds(objects_to_upload) %}
    {% elif objectset == 'snapshots' %}
        {% set content_values = dbt_demo_project.objectset_snapshots(objects_to_upload) %}
    {% elif objectset == 'sources' %}
        {% set content_values = dbt_demo_project.objectset_sources(objects_to_upload) %}
    {% elif objectset == 'tests' %}
        {% set content_values = dbt_demo_project.objectset_tests(objects_to_upload) %}
    {# Invocations only requires data from inbuild variables available in the macro #}
    {% elif objectset == 'invocations' %}
        {% set content_values = dbt_demo_project.objectset_invocations() %}
    {% endif %}

    {{ return(content_values) }}

{% endmacro %}