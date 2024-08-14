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

{% macro extract_source_nodes(model) %}
    
    {%- set selected_source_nodes = [] -%}
    
    {% if execute -%}
        {{ log('Graph --> ' ~ graph.nodes.values() | list, info=True) }}
        {%- set model_node = graph.nodes.get(model.unique_id) -%}
    {% else %}
        {%- set model_node = model -%}
    {% endif -%}
    {{ log(model_node, info=True) }}
    {%- set model_source_nodes = model_node.sources -%}
    {%- set source_node_prefix = 'source' ~ '.' ~ project_name -%}

    {% for item_source_node in model_source_nodes %}
        {%- set source_node_name = item_source_node[0] ~ '.' ~ item_source_node[1] -%}
        {%- set source_node = source_node_prefix ~ '.' ~ source_node_name -%}
        {%- set get_source_node = graph.sources.get(source_node) -%}
        
        {% do selected_source_nodes.append(get_source_node) %}
    {% endfor %}

    {{ return(selected_source_nodes) }}

{% endmacro %}

{% macro extract_source_load_config() %}

    {%- set extracted_nodes = extract_selected_nodes() -%}
    {%- set stage_yaml = fromyaml(var('stage_yaml')) -%}
    {%- set selected_source_nodes = [] -%}
    {%- set source_nodes_not_found = [] -%}
    {%- set source_node_prefix = 'source' ~ '.' ~ project_name -%}

    {% for item_source_node in extracted_nodes %}
        {% for stage_config in stage_yaml.stages %}

            --{%- set source_name = stage_config.source ~ '.' ~ stage_config.table -%}
            --{%- set source_node_name = source_node_prefix ~ '.' ~ source_name  -%}
            {{ log(stage_config, info=True) }}
            {% if item_source_node.source_name == stage_config.source and item_source_node.table_name == stage_config.table %}
                {% do selected_source_nodes.append(stage_config) %}
            {% endif %}
            
        {% endfor %}
    {% endfor %}

    {{ selected_source_nodes }}
    {{ log(selected_source_nodes, info=True) }}

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
    {{ return(graph.nodes.get(model.unique_id) if execute) }}
{% endmacro %}