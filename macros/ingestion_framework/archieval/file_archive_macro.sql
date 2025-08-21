{% macro file_archive_macro(model) %}
    {% if execute %}
    -- Log message
    {{ log('File Archive Macro', info=True) }}
    {{ log(target.name, info=true) }}

    -- Staging Locations
    {%- set stage_name = model.meta.source_location_conf.get('stage_name') -%}
    {%- set landing_path = model.meta.source_location_conf.get('stage_landing_path') -%}
    {%- set archive_path = model.meta.source_location_conf.get('stage_archive_path') -%}
    {%- set file_name = model.meta.source_location_conf.get('filename') -%}
    {%- set files = model.meta.source_location_conf.get('files') | default(None) -%}
    {%- set pattern = model.meta.source_location_conf.get('pattern') -%}
    --{% set now = modules.datetime.datetime.now() %}
    --{% set archive_time = now.strftime('%Y%m%d%H%M%S') %}
    --{% if file_name %}{{ file_name ~ '_' ~ archive_time }}{% endif %}

    -- File Archival
    {{ log('COPY FILE TO ARCHIVE FOLDER', info=True) }}
    {% call statement("file_archive_snowflake_query") %}
        COPY FILES INTO @{{ stage_name }}/{{ archive_path }}/
        FROM 
        @{{ stage_name }}/{{ landing_path }}/{% if file_name %}{{ file_name }}{% endif %}
        {% if files -%}
            -- FILES
            files = ({{ files | trim('[]') }})
        {%- endif -%}
        {% if pattern -%}
            -- PATTERN
            pattern = '{{ pattern }}'
        {%- endif -%}
    {% endcall %}

    {{ log('REMOVE FILE FROM LANDING FOLDER', info=True) }}
    {% call statement("remove_files_snowflake_query") %}
        REMOVE @{{ stage_name }}/{{ landing_path }}/{% if file_name %}{{ file_name }}{% endif %}
        {% if files -%}
            -- FILES
            files = ({{ files | trim('[]') }})
        {%- endif -%}
        {% if pattern -%}
            -- PATTERN
            pattern = '{{ pattern }}'
        {%- endif -%}
    {% endcall %}
    {% endif %}
{% endmacro %}