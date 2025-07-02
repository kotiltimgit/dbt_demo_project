{% macro upload_models() %}
    {{ "Models macro" }}
{% endmacro %}

{% macro upload_tests() %}
    {{ "Tests macro" }}
{% endmacro %}

{% macro upload_sources() %}
    {{ "Sources macro" }}
{% endmacro %}

{% macro upload_analyses() %}
    {{ "Analyses macro" }}
{% endmacro %}

{% macro type_syed() %}
    numeric(28,6)
{% endmacro %}

{% macro compare_records(src_relation, tgt_relation, pk_cols, indicator_col='indicator') %}
    {% set ns = namespace(
        N_condition='',
        D_condition='',
        select_columns='',
        checksum_expr_src='',
        checksum_expr_tgt='',
        join_condition='',
        where_condition=''
    ) %}

    {%- for pk in pk_cols -%}
        {%- set ns.D_condition -%}
            {%- if not loop.first -%}{{ ns.D_condition }} {%- endif -%}src.{{ pk }} is null{%- if not loop.last -%} AND {%- endif -%}
        {%- endset -%}

        {%- set ns.N_condition -%}
            {%- if not loop.first -%}{{ ns.N_condition }} {%- endif -%}tgt.{{ pk }} is null{%- if not loop.last -%} AND {%- endif -%}
        {%- endset -%}
    
        {%- set ns.join_condition -%}
            {%- if not loop.first -%}{{ ns.join_condition }} {%- endif -%}src.{{ pk }} = tgt.{{ pk }}{%- if not loop.last -%} AND {%- endif -%}
        {%- endset -%}

        {%- set ns.select_columns -%}
            {%- if not loop.first -%}{{ ns.select_columns }}{%- endif -%}
            coalesce(src.{{ pk }}, tgt.{{ pk }}) as {{ pk }},
        {%- endset -%}

        {%- set ns.where_condition -%}
            {%- if not loop.first -%}{{ ns.where_condition }} {%- endif -%}src.{{ pk }} is not null OR tgt.{{ pk }} is not null{%- if not loop.last -%} AND {%- endif -%}
        {%- endset -%}
    {%- endfor -%}
    

    {%- set src_cols = adapter.get_columns_in_relation(src_relation) -%}
    {%- set tgt_cols = adapter.get_columns_in_relation(tgt_relation) -%}
    {%- set common_cols = [] -%}
    
    {%- for col in src_cols -%}
        {%- if col.name != indicator_col and col.name in tgt_cols | map(attribute='name') -%}
            {%- do common_cols.append(col.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for common_column in common_cols -%}
        {%- set ns.checksum_expr_src -%}
            {%- if not loop.first -%}{{ ns.checksum_expr_src }} {%- endif -%}cast(src.{{ common_column }} as text){%- if not loop.last -%} || {%- endif -%}
        {%- endset -%}

        {%- set ns.checksum_expr_tgt -%}
            {%- if not loop.first -%}{{ ns.checksum_expr_tgt }} {%- endif -%}cast(tgt.{{ common_column }} as text){%- if not loop.last -%} || {%- endif -%}
        {%- endset -%}

        {%- set ns.select_columns -%}
            {{ ns.select_columns }}
            src.{{ common_column }} as src_{{ common_column }},
            tgt.{{ common_column }} as tgt_{{ common_column }},
        {%- endset -%}        
    {%- endfor -%}
    
    -- Generate checksum expression
    {%- set ns.checksum_expr_src = "md5(" ~ ns.checksum_expr_src ~ ")" -%}
    {%- set ns.checksum_expr_tgt = "md5(" ~ ns.checksum_expr_tgt ~ ")" -%}
    
    -- Begin SQL generation
    select
    {{ ns.select_columns }}
    case
        when {{ ns.N_condition }} then 'N'
        when {{ ns.D_condition }} then 'D'
        when {{ ns.checksum_expr_src }} = {{ ns.checksum_expr_tgt }} then 'I'
        else 'C'
    end as {{ indicator_col }}
    from {{ src_relation }} as src
    full outer join {{ tgt_relation }} as tgt
    on {{ ns.join_condition }}
    where {{ ns.where_condition }}
    ;
{% endmacro %}