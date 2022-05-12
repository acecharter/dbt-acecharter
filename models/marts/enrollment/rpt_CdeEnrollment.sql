WITH
  entity_info AS (
    SELECT DISTINCT * EXCEPT (AceComparisonSchoolCode,AceComparisonSchoolName)
    FROM {{ ref('dim_ComparisonEntities')}}
  ),
  
  enrollment AS (
    SELECT * FROM {{ ref('fct_CdeEnrollment') }}
  ),
  
  final AS (
    SELECT
      e.SchoolYear,
      i.EntityType,
      e.EntityCode,
      i.EntityName,
      i.EntityNameShort,
      e.SchoolType,
      e.SubgroupType,
      e.Subgroup,
      e.Gender,
      e.GradeLevel,
      e.Enrollment,
      e.PctOfTotalEnrollment
    FROM enrollment AS e
    LEFT JOIN entity_info AS i
    ON e.EntityCode = i.EntityCode
    WHERE
      i.EntityCode IS NOT NULL
      AND e.Enrollment IS NOT NULL
  )

SELECT * FROM final

ORDER BY 1, 3, 6, 7, 8
  