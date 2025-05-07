{% macro get_objectset_content(objectset) %}

    {% if objectset in ['model_executions', 'seed_executions', 'test_executions', 'snapshot_executions'] %}
        {# Executions make use of the results object #}
        {% set objects = results | selectattr("node.resource_type", "equalto", objectset.split("_")[0]) | list %}
    {% elif objectset in ['seeds', 'snapshots', 'tests'] %}
        {# Use the nodes in the [graph](https://docs.getdbt.com/reference/dbt-jinja-functions/graph) to extract details #}
        {% set objects = graph.nodes.values() | selectattr("resource_type", "equalto", objectset[:-1]) | list %}
    {% elif objectset in ['models'] %}
        {# Get the relation that the previous inserted results to #}
        {% set objectset_table_relation = dbt_demo_project.get_objectset_table_relation(objectset) %}

        {% set model_names_query = "SELECT NODE_ID FROM " ~ objectset_table_relation ~ " WHERE UPPER(IS_ACTIVE) = 'Y';" %}
        {{ log("QUERY ---------------------------------->      " ~ model_names_query, info=True) }}
        {% set model_names_query_result = run_query(model_names_query) %}
        {% set model_names_list = model_names_query_result.columns['NODE_ID'].values() | list %}

        {# Use the nodes in the [graph](https://docs.getdbt.com/reference/dbt-jinja-functions/graph) to extract details #}
        {% set objects = graph.nodes.values() | selectattr("resource_type", "equalto", objectset[:-1]) | selectattr("package_name", "equalto", project_name) | rejectattr("unique_id", "in", model_names_list) | list %}
    {% elif objectset in ['exposures', 'sources'] %}
        {# Use the [graph](https://docs.getdbt.com/reference/dbt-jinja-functions/graph) to extract details #}
        {% set objects = graph.get(objectset).values() | list %}
    {% elif objectset == 'invocations' %}
        {#
            Invocations doesn't need anything input, but we include this so that it will still be picked up
            as part of the loop below - the length must be > 0 to allow for an upload, hence the empty string
        #}
        {% set objects = [''] %}
    {% endif %}

    {{ return(objects) }}

{% endmacro %}