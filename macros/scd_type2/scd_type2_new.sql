{% macro scd_type2_new(source_query, natural_key, tracked_columns, all_columns) %}

WITH source_data AS (
    {{ source_query }}
),

{%- if is_incremental() %}

current_records AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

-- Detect TRACKED column changes (new version needed)
tracked_changes AS (
    SELECT
        s.*,
        CASE
            WHEN c."{{ natural_key }}" IS NULL THEN 'NEW'
            WHEN {% for col in tracked_columns %}
                NOT EQUAL_NULL(s."{{ col }}", c."{{ col }}")
                {%- if not loop.last %} OR {% endif %}
            {% endfor %} THEN 'CHANGED'
            ELSE 'NO_TRACKED_CHANGE'
        END AS _change_type
    FROM source_data s
    LEFT JOIN current_records c
        ON s."{{ natural_key }}" = c."{{ natural_key }}"
),

-- Issue 1 Fix: Detect UNTRACKED column changes (in-place update needed)
{%- set untracked_columns = all_columns | reject('equalto', natural_key) | reject('in', tracked_columns) | list %}
untracked_changes AS (
    SELECT
        c."{{ natural_key }}"
    FROM source_data s
    INNER JOIN current_records c
        ON s."{{ natural_key }}" = c."{{ natural_key }}"
    WHERE s."{{ natural_key }}" NOT IN (SELECT "{{ natural_key }}" FROM tracked_changes WHERE _change_type IN ('NEW', 'CHANGED'))
    {%- if untracked_columns | length > 0 %}
      AND (
        {% for col in untracked_columns %}
            NOT EQUAL_NULL(s."{{ col }}", c."{{ col }}")
            {%- if not loop.last %} OR {% endif %}
        {% endfor %}
      )
    {%- else %}
      AND FALSE
    {%- endif %}
),

-- In-place updates for untracked column changes (no new version)
inplace_updates AS (
    SELECT
        {% for col in all_columns %}s."{{ col }}",
        {% endfor %}
        c.valid_from,
        c.valid_to,
        c.is_current,
        c.inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM source_data s
    INNER JOIN current_records c
        ON s."{{ natural_key }}" = c."{{ natural_key }}"
    INNER JOIN untracked_changes u
        ON c."{{ natural_key }}" = u."{{ natural_key }}"
),

-- Expire old versions (tracked changes only)
expired AS (
    SELECT
        {% for col in all_columns %}c."{{ col }}",
        {% endfor %}
        c.valid_from,
        CURRENT_TIMESTAMP() AS valid_to,
        FALSE AS is_current,
        c.inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM current_records c
    INNER JOIN tracked_changes tc
        ON c."{{ natural_key }}" = tc."{{ natural_key }}"
    WHERE tc._change_type = 'CHANGED'
),

-- New versions (new + tracked changes)
new_versions AS (
    SELECT
        {% for col in all_columns %}tc."{{ col }}",
        {% endfor %}
        CURRENT_TIMESTAMP() AS valid_from,
        CAST(NULL AS TIMESTAMP_NTZ) AS valid_to,
        TRUE AS is_current,
        CURRENT_TIMESTAMP() AS inserted_date,
        CURRENT_TIMESTAMP() AS updated_date
    FROM tracked_changes tc
    WHERE tc._change_type IN ('NEW', 'CHANGED')
)

SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions
UNION ALL
SELECT * FROM inplace_updates

{%- else %}

-- Full load (first run)
initial_load AS (
    SELECT
        {% for col in all_columns %}"{{ col }}",
        {% endfor %}
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