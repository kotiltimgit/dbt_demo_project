{{
    config(
        materialized='table',
        database='DBT_DB_DEV',
        schema='SILVER',
        alias='JOB_SCHEDULES'
    )
}}

SELECT 
  'MATILLION' AS PLATFORM_NAME,
  "NAME" AS SCHEDULE_NAME, 
  "JOBNAME" AS JOB_NAME,  
  'ORCHESTRATION' AS "JOB_TYPE" ,
  "ENVIRONMENTNAME",
  "VERSIONNAME",
  "ENABLED", 
  "DAYSOFMONTH",
  "DAYOFWEEK", 
  "SUNDAY", 
  "MONDAY",
  "TUESDAY",
  "WEDNESDAY",
  "THURSDAY",
  "FRIDAY",
  "SATURDAY", 
  "HOUR", 
  "MINUTE",
  "TIMEZONE",  
  CURRENT_USER() AS "INSERTED_BY", 
  CURRENT_USER() AS "UPDATED_BY", 

  current_timestamp() AS "INSERT_DATE", 
  current_timestamp() AS "UPDATE_DATE", 
FROM {{ source('MATILLION', 'MATILLION_SCHEDULES') }}