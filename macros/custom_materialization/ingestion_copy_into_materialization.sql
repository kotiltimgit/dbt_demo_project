{% materialization copy_into_materialization, adapter='default' -%}

    -- Log message
    {{ log('USING INGESTION MATERIALIZATION', info=True) }}

    -- relations
    {%- set table_name = config.require('alias') -%}
    {%- set schema = config.require('schema') -%}
    {%- set database = config.require('database') -%}
    {%- set target_relation = database ~ '.' ~ schema ~ '.' ~ table_name -%}

    -- external stage
    {%- set stage_name = config.require('external_stage') -%}
    {%- set stage_file_path = config.get('stage_file_path', default=None) -%}    -- for single file
    {%- set files = config.get('files', default=None) -%}
    {%- set pattern = config.get('pattern', default=None) -%}
    --{%- set external_stage = '@' ~ stage_name ~ '/' ~ stage_file_path -%}

    -- file format
    {%- set file_format_name = config.require('file_format') -%}
    -- copy options
    {%- set copy_options = config.get('copy_options', default=None) -%}
    -- validation_mode
    {%- set validation_mode = config.get('validation_mode', default=None) -%}

    {% call statement("main") %}
        copy into {{ target_relation }}
            from '@{{ stage_name }}
            {%- if stage_file_path -%}/{{ stage_file_path }}{%- endif -%}'
        {% if files -%}
        files = ({{ files | trim('[]') }})
        {%- endif -%}
        {% if pattern -%}
        pattern = '{{ pattern }}'
        {%- endif -%}
        -- FILE FORMAT
        file_format = (format_name = '{{ file_format_name }}')
        -- COPY OPTIONS
        {% if copy_options -%}
            {{ copy_options.items() | map('join', ' = ') | join('\n') }}
        {% endif -%}
        -- VALIDATION MODE
        {% if validation_mode -%}
        validation_mode = '{{ validation_mode }}'
        {%- endif -%}
        ;
    {% endcall %}

    {{ return({'relations': [this]}) }}

{%- endmaterialization %}