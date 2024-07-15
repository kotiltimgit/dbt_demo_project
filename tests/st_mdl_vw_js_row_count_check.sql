SELECT
    COUNT(*)
FROM {{ ref('vw_job_schedules') }}
WHERE PLATFORM_NAME = 'MATILLION'

MINUS

SELECT
    COUNT(*)
FROM {{ ref('job_schedules') }}
WHERE PLATFORM_NAME = 'MATILLION'