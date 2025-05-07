{% macro upload_objects_collection(results) %}

    {% if execute %}
        {% set objects_to_upload = ['exposures', 'seeds', 'snapshots', 'invocations', 'sources', 'tests', 'models'] %}
        {% if results %}
            {# When results is not empty, then some executions are there in results. So includig excutions also for loading #}
            {% set objects_to_upload = ['model_executions', 'seed_executions', 'test_executions', 'snapshot_executions'] + objects_to_upload %}
        {% endif %}

        {# Load each object set #}
        {% for objectset in objects_to_upload %}

            {{ log("Loading '" ~ objectset.replace("_", " ") ~ "' into the Data Warehouse", info=True) }}

            {# Get the collection that need to be loaded #}
            {% set objects = dbt_demo_project.get_objectset_content(objectset) %}

            {# Load in chunks to reduce the query size #}
            {% if objectset == 'models' %}
                {% set upload_size = 100 %}
            {% else %}
                {% set upload_size = 5000 %}
            {% endif %}

            {# Iterate each chunk #}
            {% for i in range(0, objects | length, upload_size) -%}

                {# Get just the objectset to load on this loop #}
                {% set objectset_content = dbt_demo_project.get_objectset_table_content_values(objectset, objects[i: i + upload_size]) %}

                {# Insert the objectset into the objects metadata table #}
                {% if objectset_content %}

                    {# Get the relation that the results will be uploaded to #}
                    {% set objectset_relation = dbt_demo_project.get_objectset_table_relation(objectset) %}
                    {# Get the column names list for the objectset to be inserted #}
                    {% set objectset_columns = dbt_demo_project.get_objectset_column_names_list(objectset) %}

                    {# Insert the data into the table #}
                    {% set insert_into_table_sql %}
                    insert into {{ objectset_relation }} {{ objectset_columns }}
                    {{ objectset_content }}
                    {% endset %}

                    {% do run_query(insert_into_table_sql) %}

                {% endif %}

            {# Loop the next 'chunk' #}
            {% endfor %}

        {# Loop the next 'objectset' #}
        {% endfor %}
        
    {% endif %}
    
{% endmacro %}