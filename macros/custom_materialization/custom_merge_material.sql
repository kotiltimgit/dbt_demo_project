{% materialization custom_merge_material, adapter='default' -%}
 
  -- Log message
  {{ log('USING CUSTOM MATERIALIZATION', info=True) }}
 
  -- config
  {%- set database = config.require('database') -%}
  {%- set schema = config.require('schema') -%}
  {%- set table_name = config.require('alias') -%}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set target = database ~ '.' ~ schema ~ '.' ~ table_name -%}
 
  {%- set update_exclude_columns = config.require('exclude_update') -%}
  {%- set update_exclude_columns_list = "('" + update_exclude_columns | join("', '") + "')" -%}
  {%- set insert_exclude_columns = config.require('exclude_insert') -%}
  {%- set insert_exclude_columns_list = "('" + insert_exclude_columns | join("', '") + "')" -%}

  -- extracting the target table columns and storing in list
  {%- set  update_result = run_custom_query(
    "select COLUMN_NAME from " ~ database ~ ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" ~ schema ~ "' AND TABLE_NAME='" ~ table_name ~ "' AND COLUMN_NAME NOT IN " ~ update_exclude_columns_list ~ " ORDER BY ORDINAL_POSITION;") -%}
  {%- set update_columns = update_result['data'] | map(attribute=0) | list -%}

  {%- set  insert_result = run_custom_query(
    "select COLUMN_NAME from " ~ database ~ ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='" ~ schema ~ "' AND TABLE_NAME='" ~ table_name ~ "' AND COLUMN_NAME NOT IN " ~ insert_exclude_columns_list ~ " ORDER BY ORDINAL_POSITION;") -%}
  {%- set insert_columns_list = insert_result['data'] | map(attribute=0) | list -%}
  {%- set insert_columns = '"' + insert_columns_list | join('", "') + '"' -%}
  {%- set values_columns = insert_columns -%}
 
  {%- set join_condition = [] -%}
  {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
    {% for key in unique_key %}
        {% set this_key_match %}
            target."{{ key }}" = source."{{ key }}"
        {% endset %}
        {% do join_condition.append(this_key_match) %}
    {% endfor %}
  {% else %}
    {% set unique_key_match %}
        target."{{ unique_key }}" = source."{{ unique_key }}"
    {% endset %}
    {% do join_condition.append(unique_key_match) %}
  {% endif %}
 
  {%- set source = sql -%}
 
  {% call statement("main") %}
    merge into {{ target }} as target
    using ({{ source }}) as source
    on {{"(" ~ join_condition | join(") and (") ~ ")"}}
 
    when matched then update set
        {% for column_name in update_columns -%}
            "{{ column_name }}" = source."{{ column_name }}"
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
 
    when not matched then insert
        ({{ insert_columns }})
    values
        ({{ values_columns }})
  {% endcall %}
 
  {{ return({'relations': [this]}) }}
 
{% endmaterialization %}  