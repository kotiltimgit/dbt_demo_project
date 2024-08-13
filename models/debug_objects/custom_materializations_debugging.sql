{{
    config(
        materialized='copy_into_materialization',
        alias='MATILLION_SCHEDULES',
        database='DBT_PROD',
        schema='DEVELOPMENT_PUBLIC',
        external_stage='REST_API_INTEGRATION.API_INTEGRATION.AWS_STAGE',
        stage_file_path='DBT_METADATA/CSV/List Users.csv',
        file_format='REST_API_INTEGRATION.API_INTEGRATION.CSV_FORMAT'
    )
}}

----------------------------------------------------------------------------

{% set value={'target_columns': "all"} %}
{{True if value.target_columns == 'all' else False}}
{% set check = value.target_columns | join(', ') if value.target_columns != 'all' else None %}
{{check}}
{%- set target_relation = this -%}
{%- set load_config = {'stage': '<stage_name>', 'location_path': '<relative_path>', 'files': ['<file_name_1>', '<file_name_2>', '...'], 'pattern': '<regex_pattern>', 'file_format': '<file_format_name>', 'columns': {'source_columns': "all | ['<source_column_1>', '<source_column_2>', ...]", 'target_columns': "all | ['<target_column_1>', '<target_column_2>', ...]"}, 'copy_options': {'on_error': "CONTINUE | SKIP_FILE | SKIP_FILE_<num> | 'SKIP_FILE_<number>%' | ABORT_STATEMENT", 'size_limit': '<number>', 'purge': 'true | false', 'return_failed_only': 'true | false', 'match_by_column_name': 'CASE_SENSITIVE | CASE_INSENSITIVE | NONE', 'include_metadata': {'metadata_columns': ['<metadata_column_1>', '<metadata_column_2>', '...'], 'target_columns': ['<target_column_1>', '<target_column_2>', '...']}, 'enforce_length': 'true | false', 'truncatecolumns': 'true | false', 'force': 'true | false', 'load_uncertain_files': 'true | false'}, 'validation_mode': 'RETURN_<number>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS'} -%} 

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
pattern = {{ load_config.pattern }}
{% endif -%}
-- FILE FORMAT
file_format = (format_name = '{{load_config.file_format}}')
-- COPY OPTIONS
{% if load_config.copy_options -%}
{{ load_config.copy_options.items() | map('join', ' = ') | join('\n') }}
{% endif -%}
-- VALIDATION MODE
{% if load_config.validation_mode -%}
validation_mode = {{ load_config.validation_mode }}
{% endif -%}
;

{{graph.nodes.values() | list | join(',\n')}}
{% set v1={'source': 'MATILLION', 'table': 'MATILLION_SCHEDULES'} %}
{{source(v1.source, v1.table)}}

{{graph.sources.get('source.dbt_demo_project.MATILLION.MATILLION_SCHEDULES') }}

