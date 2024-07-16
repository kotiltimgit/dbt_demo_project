SELECT
    COUNT(*)
FROM {{ ref('job_schedules') }}
WHERE PLATFORM_NAME = 'MATILLION'

MINUS 

SELECT
    COUNT(*)
FROM {{ source('MATILLION', 'MATILLION_SCHEDULES') }}