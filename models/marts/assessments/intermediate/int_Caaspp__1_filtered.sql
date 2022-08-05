{{ config(
    materialized='table'
)}}

SELECT *
FROM {{ ref('stg_RD__Caaspp')}}
WHERE
  GradeLevel >= 5
  AND DemographicId IN (
    '1',   --All Students
    '128', --Reported Disabilities
    '31',  --Economic disadvantaged
    '160', --EL (English learner)
    '78',  --Hispanic or Latino
    '204'  --Economically disadvantaged Hispanic or Latino
    )