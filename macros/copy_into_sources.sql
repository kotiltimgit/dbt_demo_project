{% macro loading_into_source() %}

    {%- set selected_load_configs = extract_source_nodes(model) -%}
    
    {% if selected_load_configs %}
        {{ log('PRE_HOOK EXECUTIONS START', info=True) }}

        {% for item_load_config in selected_load_configs %}
            {%- set target_relation = item_load_config.relation_name -%}
            {%- set load_config = item_load_config.external.load_config -%}

            {% call statement() %}
                copy into {{ target_relation }}
                    from '@{{ load_config.stage }}
                {%- if load_config.location_path -%}
                /{{ load_config.location_path }}
                {%- endif -%}'
                -- FILES
                {% if load_config.files -%}
                files = ({{ load_config.files | trim('[]') }})
                {% endif -%}
                -- PATTERN
                {% if load_config.pattern -%}
                pattern = '{{ load_config.pattern }}'
                {% endif -%}
                -- FILE FORMAT
                file_format = (format_name = '{{load_config.file_format}}')
                -- COPY OPTIONS
                {% if load_config.copy_options -%}
                {{ load_config.copy_options.items() | map('join', ' = ') | join(', ') }}
                {% endif -%}
                -- VALIDATION MODE
                {% if load_config.validation_mode -%}
                validation_mode = {{ load_config.validation_mode }}
                {% endif -%}
                ;
            {% endcall %}

            {{ log('PRE_HOOK_' ~ loop.index ~ ' COMPLETED', info=True) }}
        {% endfor %}
        {{ log('PRE_HOOK EXECUTIONS END', info=True) }}

    {% else %}
        {{ log('INGESTION PRE_HOOKS NOT FOUND', info=True) }}

    {% endif %}

{% endmacro %}

{% macro extract_source_nodes(model_sources) %}
    
    {%- set selected_source_nodes = [] -%}
    {%- set extracted_model_nodes = extract_nodes() -%}
    
    /*{% if execute -%}
        {{ log('Graph --> ' ~ graph.nodes.values() | list, info=True) }}
        {%- set model_node = graph.nodes.get(model.unique_id) -%}
    {% else %}
        {%- set model_node = model -%}
    {% endif -%}
    {{ log(model_node, info=True) }}*/
    {% for item_model_node in extracted_model_nodes %}
        {%- set model_source_nodes = item_model_node.sources -%}
        {%- set source_node_prefix = 'source' ~ '.' ~ project_name -%}

        {% for item_source_node in model_source_nodes %}
            {%- set source_node_name = item_source_node[0] ~ '.' ~ item_source_node[1] -%}
            {%- set source_node = source_node_prefix ~ '.' ~ source_node_name -%}
            {%- set get_source_node = graph.sources.get(source_node) -%}
            
            {% do selected_source_nodes.append(get_source_node) %}
        {% endfor %}
    {% endfor %}
    
    {{ return(selected_source_nodes) }}

{% endmacro %}

{% macro extract_nodes() %}
    --{{ log(selected_resources, info=True) }}
    {%- set extracted_nodes = [] -%}
    {% for item_selected_node in selected_resources %}
        {% for item_graph_node in graph.nodes.values() | selectattr("unique_id", "equalto", item_selected_node) %}
            {% if item_graph_node.sources %}
                {% do extracted_nodes.append(item_graph_node) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    
    {{ return(extracted_nodes) }}

{% endmacro %}

{% macro hook_macro() %}
    {% set l = ['SELECT 25 as AGE', "SELECT 'Male' as GENDER"] %}
    {% for item in l %}
        {% call statement(auto_begin=True) %}
            {{item}}
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro nodes_list() %}
    {% if execute %}
        {% set l = [] %}
        {{ log("Executing", info=True) }}
        {% for item in graph.nodes.values() | selectattr("unique_id", "equalto", model.unique_id) %}
            {% do l.append(item) %}
        {% endfor %}
        {{ return(l) }}
    {% else %}
        {{ log("Not Executing", info=True) }}
    {% endif %}
{% endmacro %}