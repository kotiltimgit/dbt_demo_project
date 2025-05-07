{% macro get_objectset_table_relation(objectset) %}
    {% if execute %}
        {% set model_relation_unique_id = "model.dbt_demo_project." ~ objectset %}
        {% set model_get_relation_node = graph.nodes.values() | selectattr('unique_id', 'equalto', model_relation_unique_id) | first %}
        {% set relation = api.Relation.create(
            database = model_get_relation_node.database,
            schema = model_get_relation_node.schema,
            identifier = model_get_relation_node.alias
        )
        %}
        {% do return(relation) %}
    {% else %}
        {% do return(api.Relation.create()) %}
    {% endif %}
{% endmacro %}