{% macro ingestion_csv_macro(model) %}
    {% if execute %}
    -- Log message
    {{ log('INGESTION FRAMEWORK', info=True) }}
    {{ log(target.name, info=true) }}

    -- Stage relation
    {%- set stg_table_name = model.meta.stage_table.get('stage_table_name') -%}
    {%- set stg_schema = model.meta.stage_table.get('schema') -%}
    {%- set stg_database = model.meta.stage_table.get('database') -%}
    {%- set stage_table_relation = stg_database ~ '.' ~ stg_schema ~ '.' ~ stg_table_name -%}

    {%- set columns_definition = model.get('columns') -%}
    {%- set primary_keys = model.config.get('primary_keys') -%}
    {%- set stage_table_flag = model.meta.stage_table.get('enabled') -%}

    -- external stage
    {%- set stage_name = model.meta.source_location_conf.get('stage_name') -%}
    -- For single file: Path must be point out to the file (e.g. - 'path/to/the/file.csv' [OR] 'path/to/the/file.json' [OR] .....)
    -- For multiple files: Path must be point out to the folder/directory (e.g. - 'path/to/the/directory')
    {%- set location_path = model.meta.source_location_conf.get('stage_landing_path') -%}
    {%- set file_name = model.meta.source_location_conf.get('filename') -%}
    {%- set files = model.meta.source_location_conf.get('files') | default(None) -%}
    {%- set pattern = model.meta.source_location_conf.get('pattern') -%}
    --{%- set external_stage = '@' ~ stage_name ~ '/' ~ stage_file_path -%}

    -- file format
    {%- set file_format_name = model.meta.source_location_conf.get('file_format') -%}
    -- copy options
    {%- set copy_options = model.meta.source_location_conf.get('copy_options') -%}
    -- validation_mode
    {%- set validation_mode = model.meta.source_location_conf.get('validation_mode') -%}

    {%- set column_ddl = ddl_column_definition(columns_definition, primary_keys) -%}

    {% if stage_table_flag %}
        {{ log("Executing 'CREATE TABLE SQL' for Stage Table", info=True) }}
        {% call statement('stage_table_create_sql') %}
            create table if not exists {{ stage_table_relation }} (
                {{ column_ddl }}
            )
            ;
        {% endcall %}

        {{ log("Executing 'TRUNCATE TABLE SQL' for Stage Table", info=True) }}
        {% call statement('stage_table_truncate_sql') %}
            truncate table {{ stage_table_relation }}
            ;
        {% endcall %}

        {{ log("Executing 'COPY INTO SQL' for Stage Table", info=True) }}
        {% call statement("stage_table_copy_into_sql") %}
            copy into {{ stage_table_relation }}({{ columns_definition.values() | map(attribute='name') | join(', ') }})
                from (
                    select
                    {% for col in columns_definition.values() %}
                        {% if col.meta.copy_transformation_logic %}
                            {{ col.meta.copy_transformation_logic | replace(col.meta.source_column_position, "file." ~ col.meta.source_column_position) }}{% if not loop.last %}, {% endif %}
                        {% else %}
                            file.{{ col.meta.source_column_position }}{% if not loop.last %}, {% endif %}
                        {% endif %}
                    {% endfor %}
                    from '@{{ stage_name }}/{{ location_path }}{% if file_name %}/{{ file_name }}{% endif %}' file
                )
            {% if files -%}
            -- FILES
            files = ({{ files | trim('[]') }})
            {%- endif -%}
            {% if pattern -%}
            -- PATTERN
            pattern = '{{ pattern }}'
            {%- endif -%}
            -- FILE FORMAT
            file_format = (format_name = '{{ file_format_name }}')
            {% if copy_options -%}
            -- COPY OPTIONS
            {{ copy_options }}
            {% endif -%}
            {% if validation_mode -%}
            -- VALIDATION MODE
            validation_mode = '{{ validation_mode }}'
            {%- endif -%}
            ;
        {% endcall %}
    {% endif %}

    -- Raw relation
    {%- set raw_table_name = model.config.get('raw_table_name') -%}
    {%- set raw_schema = model.config.get('schema') -%}
    {%- set raw_database = model.config.get('database') -%}
    {%- set raw_table_relation = raw_database ~ '.' ~ raw_schema ~ '.' ~ raw_table_name -%}

    {{ log("Executing 'CREATE TABLE SQL' for Raw Table", info=True) }}
    {% call statement('raw_table_create_sql') %}
        create table if not exists {{ raw_table_relation }} (
            {{ column_ddl }}
        )
        ;
    {% endcall %}

    {% if stage_table_flag %}
        {%- set join_condition = [] -%}
        {% if primary_keys is sequence and primary_keys is not mapping and primary_keys is not string %}
            {% for key in primary_keys %}
                {% set this_key_match %}
                    target."{{ key }}" = source."{{ key }}"
                {% endset %}
                {% do join_condition.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                target."{{ primary_keys }}" = source."{{ primary_keys }}"
            {% endset %}
            {% do join_condition.append(unique_key_match) %}
        {% endif %}

        {{ log("Executing 'MERGE INTO SQL' for Raw Table", info=True) }}
        {% call statement("raw_table_merge_into_sql") %}
            merge into {{ raw_table_relation }} as target
            using (
                select
                {% for col in columns_definition.values() %}
                    {% if col.meta.transformation_logic %}
                        {{ col.meta.transformation_logic }} as {{ col.name }}{% if not loop.last %}, {% endif %}
                    {% else %}
                        {{ col.name }}{% if not loop.last %}, {% endif %}
                    {% endif %}
                {% endfor %}
                from {{ stage_table_relation }}
            ) as source
            on {{"(" ~ join_condition | join(") and (") ~ ")"}}
        
            when matched then
            update set
            {% for column_name in columns_definition.values() -%}
                target."{{ column_name.name }}" = source."{{ column_name.name }}"
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
        
            when not matched then
            insert(
                {{ columns_definition.values() | map(attribute='name') | join(', ') }}
            )
            values(
                {% for column_name in columns_definition.values() -%}
                    source.{{ column_name.name }}
                    {%- if not loop.last %}, {%- endif %}
                {%- endfor %}
            )
            ;
        {% endcall %}
    
    {% else %}
        {{ log("Executing 'COPY INTO SQL' for Raw Table", info=True) }}
        {% call statement("raw_table_copy_into_sql") %}
            copy into {{ raw_table_relation }}({{ columns_definition.values() | map(attribute='name') | join(', ') }})
                from (
                    select
                    {% for col in columns_definition.values() %}
                        {% if col.meta.copy_transformation_logic %}
                            {{ col.meta.copy_transformation_logic | replace(col.meta.source_column_position, "file." ~ col.meta.source_column_position) }}{% if not loop.last %}, {% endif %}
                        {% else %}
                            file.{{ col.meta.source_column_position }}{% if not loop.last %}, {% endif %}
                        {% endif %}
                    {% endfor %}
                    from '@{{ stage_name }}/{{ location_path }}{% if file_name %}/{{ file_name }}{% endif %}' file
                )
            {% if files -%}
            -- FILES
            files = ({{ files | trim('[]') }})
            {%- endif -%}
            {% if pattern -%}
            -- PATTERN
            pattern = '{{ pattern }}'
            {%- endif -%}
            -- FILE FORMAT
            file_format = (format_name = '{{ file_format_name }}')
            {% if copy_options -%}
            -- COPY OPTIONS
            {{ copy_options }}
            {% endif -%}
            {% if validation_mode -%}
            -- VALIDATION MODE
            validation_mode = '{{ validation_mode }}'
            {%- endif -%}
            ;
        {% endcall %}
    {% endif %}

    {% endif %}

{% endmacro %}

{% macro ddl_column_definition(column_definition, primary_key_columns) %}
    {% for col_def in column_definition.values() %}
        {{ col_def.name }} {{ col_def.meta.sql_column_datatype }},
    {% endfor %}
    primary key ({{ primary_key_columns | join(", ") }})
{% endmacro %}

{% macro select_clause_create(column_definition) %}
    select
    {% for col_def in column_definition.values() %}
        {{ col_def.name }}{% if not loop.last %},{% endif %}
    {% endfor %}
    
{% endmacro %}


{% macro ingestion_macro1(model) %}
    {% if execute %}
    -- Log message
    {{ log('INGESTION FRAMEWORK', info=True) }}
    {{ log(model, info=true) }}

    -- Stage relation
    {%- set stg_table_name = model.meta.stage_table.get('stage_table_name') -%}
    {%- set stg_schema = model.meta.stage_table.get('schema') -%}
    {%- set stg_database = model.meta.stage_table.get('database') -%}
    {%- set stage_table_relation = stg_database ~ '.' ~ stg_schema ~ '.' ~ stg_table_name -%}
    {{log(stage_table_relation, info=true)}}

    {% endif %}

{% endmacro %}

{% macro prehook_macro() %}
    {% for res in results %}
        select {{ res.node.unique_id }} as node, {{ res.status }} as status
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endmacro %}

{% macro posthook_macro() %}
{% if execute %}
    {% for res in results %}
        select {{ res.node.unique_id }} as node, {{ res.status }} as status
        union all
    {% endfor %}
    select 'model_name' as node, 'Failed' as status
{% endif %}
{% endmacro %}