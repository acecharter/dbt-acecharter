{{ config(
    materialized='table'
)}}

WITH 
  comparison_entities AS (
    SELECT DISTINCT * EXCEPT(AceComparisonSchoolCode, AceComparisonSchoolName)
    FROM {{ ref('dim_ComparisonEntities')}}
  ),

  elpac AS (
    SELECT *
    FROM {{ ref('int_Elpac__unioned')}}
    WHERE
      GradeLevel >= 5
      AND EntityCode IN (SELECT EntityCode FROM comparison_entities)
      AND DemographicId IN (
        '160', --All English learners - (All ELs) (Same as All Students (code 1) for ELPAC files)
        '120', --ELs enrolled less than 12 months
        '142', --ELs enrolled 12 months or more
        '242', --EL - 1 year in program
        '243', --EL - 2 years in program
        '244', --EL - 3 years in program
        '245', --EL - 4 years in program
        '246', --EL - 5 years in program
        '247' --EL - 6+ years in program
      )
  ),

  final AS (
    SELECT
      e.*,
      c.* EXCEPT (EntityCode),
    FROM elpac AS e
    LEFT JOIN comparison_entities AS c
    ON e.EntityCode = c.EntityCode

  )

SELECT * FROM final
