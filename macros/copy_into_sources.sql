{% macro loading_into_source() %}

    {%- set extracted_model_nodes = [] -%}
    --{{ log('Selected_resources' ~ selected_resources, info=True) }}
    {% for item_selected_node in selected_resources %}
        {%- set model_node = graph.nodes.get(item_selected_node) -%}
        --{{ log('model_node' ~ model_node, info=True) }}
        {% if model_node.sources %}
            {% do extracted_model_nodes.append(model_node) %}
        {% endif %}
    {% endfor %}

    {%- set selected_source_nodes = [] -%}
    {{ log('extracted_model_nodes' ~ extracted_model_nodes, info=True) }}
    
    {% if extracted_model_nodes %}
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
    {% endif %}


    {%- set extracted_nodes_list = extract_nodes() -%}
    {{ log('extracted_nodes_list' ~ extracted_nodes_list, info=True) }}
    {%- set selected_load_configs = extract_source_nodes(extracted_nodes_list) -%}
    {{ log('selected_load_configs' ~ selected_load_configs, info=True) }}
    
    {% if selected_load_configs %}
        {{ log('INGESTION EXECUTIONS START', info=True) }}

        {% for item_load_config in selected_load_configs %}
            {%- set target_relation = item_load_config.relation_name -%}
            {%- set load_config = item_load_config.external.load_config -%}
            {% if load_config.columns -%}
                {%- set src_columns = load_config.columns.get('source_columns') -%}
                {%- set tgt_columns = load_config.columns.get('target_columns') -%}
            {% endif -%}

            {% call statement() %}
                copy into {{ target_relation }} {%- if load_config.columns -%}
                ({{ tgt_columns | join(', ') }}) 
                    from (
                            select
                                {{ src_columns | join(', ') }}
                            from '@{{ load_config.stage }}
                            {%- if load_config.location_path -%}
                            /{{ load_config.location_path }}
                            {%- endif -%}'
                        )
                    
                {%- else -%}
                    from '@{{ load_config.stage }}
                    {%- if load_config.location_path -%}
                    /{{ load_config.location_path }}
                    {%- endif -%}'
                {%- endif -%}
                {% if load_config.files -%}.
                -- FILES
                files = ({{ load_config.files | trim('[]') }})
                {% endif -%}
                {% if load_config.pattern -%}
                -- PATTERN
                pattern = '{{ load_config.pattern }}'
                {% endif -%}
                -- FILE FORMAT
                file_format = (format_name = '{{load_config.file_format}}')
                {% if load_config.copy_options -%}
                -- COPY OPTIONS
                {{ load_config.copy_options.items() | map('join', ' = ') | join('\n') }}
                {% endif -%}
                {% if load_config.validation_mode -%}
                -- VALIDATION MODE
                validation_mode = {{ load_config.validation_mode }}
                {% endif -%}
                ;
            {% endcall %}

            {{ log('INGESTION_' ~ loop.index ~ ' COMPLETED', info=True) }}
        {% endfor %}
        {{ log('INGESTION EXECUTIONS END', info=True) }}

    {% else %}
        {{ log('INGESTION HOOKS NOT FOUND', info=True) }}

    {% endif %}

{% endmacro %}

{% macro extract_source_nodes(extracted_model_nodes) %}
    
    {%- set selected_source_nodes = [] -%}
    {{ log('extracted_model_nodes' ~ extracted_model_nodes, info=True) }}
    
    {% if extracted_model_nodes %}
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
    {% endif %}
    --{{ log('selected_source_nodes' ~ selected_source_nodes, info=True) }}
    {{ return(selected_source_nodes) }}

{% endmacro %}

{% macro extract_nodes() %}

    {% if execute %}
        {%- set extracted_nodes = [] -%}
        --{{ log('Selected_resources' ~ selected_resources, info=True) }}
        {% for item_selected_node in selected_resources %}
            {%- set model_node = graph.nodes.get(item_selected_node) -%}
            --{{ log('model_node' ~ model_node, info=True) }}
            {% if model_node.sources %}
                {% do extracted_nodes.append(model_node) %}
            {% endif %}
        {% endfor %}

        {{ return(extracted_nodes) }}
    {% endif %}

{% endmacro %}

{% macro get_SQL() %}
    {{ return(this.sql) }}
{% endmacro %}