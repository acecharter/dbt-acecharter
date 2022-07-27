WITH
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
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
    LEFT JOIN entities AS i
    ON e.EntityCode = i.EntityCode
    WHERE
      i.EntityCode IS NOT NULL
      AND e.Enrollment IS NOT NULL
  )

SELECT * FROM final

ORDER BY 1, 3, 6, 7, 8
  