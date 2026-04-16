{% macro scd_type2(source_query, natural_key, tracked_columns, all_columns) %}

{%- set hash_columns = tracked_columns | join("', '") -%}

WITH source_data AS (
    {{ source_query }}
),

{%- if is_incremental() %}

current_records AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

changes AS (
    SELECT
        s.*,
        MD5(CONCAT_WS('|', {% for col in tracked_columns %}s."{{ col }}"{% if not loop.last %}, {% endif %}{% endfor %})) AS _source_hash,
        c._row_hash AS _existing_hash
    FROM source_data s
    LEFT JOIN current_records c
        ON s."{{ natural_key }}" = c."{{ natural_key }}"
    WHERE c."{{ natural_key }}" IS NULL
       OR MD5(CONCAT_WS('|', {% for col in tracked_columns %}s."{{ col }}"{% if not loop.last %}, {% endif %}{% endfor %})) != c._row_hash
),

-- Expire old versions
expired AS (
    SELECT
        {% for col in all_columns %}c."{{ col }}",
        {% endfor %}
        c._row_hash,
        c.valid_from,
        CURRENT_TIMESTAMP() AS valid_to,
        FALSE AS is_current,
        c.inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM current_records c
    INNER JOIN changes ch
        ON c."{{ natural_key }}" = ch."{{ natural_key }}"
),

-- New versions
new_versions AS (
    SELECT
        {% for col in all_columns %}ch."{{ col }}",
        {% endfor %}
        MD5(CONCAT_WS('|', {% for col in tracked_columns %}ch."{{ col }}"{% if not loop.last %}, {% endif %}{% endfor %})) AS _row_hash,
        CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP_NTZ) AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM changes ch
)

SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions

{%- else %}

-- Full load (first run)
initial_load AS (
    SELECT
        {% for col in all_columns %}"{{ col }}",
        {% endfor %}
        MD5(CONCAT_WS('|', {% for col in tracked_columns %}"{{ col }}"{% if not loop.last %}, {% endif %}{% endfor %})) AS _row_hash,
        CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP_NTZ) AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM source_data
)

SELECT * FROM initial_load

{%- endif %}

{% endmacro %}