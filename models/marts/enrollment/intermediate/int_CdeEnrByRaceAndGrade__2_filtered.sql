WITH 
  entities AS (
    SELECT DISTINCT * EXCEPT (AceComparisonSchoolCode,AceComparisonSchoolName)
    FROM {{ ref('dim_ComparisonEntities')}}
  ),

  enrollment AS (
    SELECT * FROM {{ ref('int_CdeEnrByRaceAndGrade__1_unioned') }}
  ),

  final AS (
    SELECT
      enr.*
    FROM enrollment AS enr
    LEFT JOIN entities
    ON enr.EntityCode = entities.EntityCode
    WHERE entities.EntityCode IS NOT NULL
  )

SELECT * FROM final
