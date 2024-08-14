/*
For this model, the below command needs to be passed along with dbt commands
[   --vars '{MAT_ALIAS_NAME: JOB_SCHEDULES}'   ]
*/

{{
    config(
        materialized='custom_merge_material',
        alias=var('MAT_ALIAS_NAME', default='JOB_SCHEDULES'),
        database='DBT_DB_DEV',
        schema='SILVER',
        unique_key=['PLATFORM_NAME','SCHEDULE_NAME','JOB_NAME'],
        exclude_update=['JOB_SCHEDULE_ID','INSERTED_BY','INSERT_DATE'],
        exclude_insert=['JOB_SCHEDULE_ID'],
        pre_hook="{{loading_into_source(model.sources)}}"
    )
}}

SELECT 
  'MATILLION' AS PLATFORM_NAME,
  "NAME" AS SCHEDULE_NAME, 
  "JOBNAME" AS JOB_NAME,  
  'ORCHESTRATION' AS "JOB_TYPE" ,
  "ENVIRONMENTNAME",
  "VERSIONNAME",
  NULL AS PROJECTNAME,
  NULL AS COMMAND_DETAILS,
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
  NULL AS INTERVAL,
  NULL AS FREQUENCY, 
  "TIMEZONE",  
  NULL AS CRON_SCHEDULE,
  NULL AS CREATED, 
  NULL AS LASTALTERED,
  NULL AS DELETED, 
  NULL AS IS_AUTOINGEST_ENABLED,
  CURRENT_USER() AS "INSERTED_BY", 
  CURRENT_USER() AS "UPDATED_BY", 
  NULL AS COMMENTS,
  current_timestamp() AS "INSERT_DATE", 
  current_timestamp() AS "UPDATE_DATE", 
FROM {{ source('MATILLION', 'MATILLION_SCHEDULES') }}
