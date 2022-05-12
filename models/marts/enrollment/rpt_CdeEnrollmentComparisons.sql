WITH
  entities AS (
    SELECT * FROM {{ ref('dim_ComparisonEntities')}}
  ),

  entity_info AS (
    SELECT DISTINCT * EXCEPT (AceComparisonSchoolCode,AceComparisonSchoolName)
    FROM entities
  ),
  
  enrollment AS (
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
    FROM {{ ref('fct_CdeEnrollment') }} AS e
    LEFT JOIN entity_info AS i
    ON e.EntityCode = i.EntityCode
    WHERE
      i.EntityCode IS NOT NULL
      AND e.Enrollment IS NOT NULL
  ),

  empower AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      enr.*
    FROM entities AS e
    LEFT JOIN enrollment AS enr
    ON e.EntityCode = enr.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0116814'
      AND GradeLevel IN ('5', '6', '7', '8', 'All')
  ),

  esperanza AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      enr.*
    FROM entities AS e
    LEFT JOIN enrollment AS enr
    ON e.EntityCode = enr.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0129247'
      AND GradeLevel IN ('5', '6', '7', '8', 'All')
  ),

  inspire AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      enr.*
    FROM entities AS e
    LEFT JOIN enrollment AS enr
    ON e.EntityCode = enr.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0131656'
      AND GradeLevel IN ('5', '6', '7', '8', 'All')
  ),

  ace_hs AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      enr.*
    FROM entities AS e
    LEFT JOIN enrollment AS enr
    ON e.EntityCode = enr.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0125617'
      AND GradeLevel IN ('9', '10', '11', '12', 'All')
  ),
  
  final AS (
    SELECT * FROM empower
    UNION ALL
    SELECT * FROM esperanza
    UNION ALL
    SELECT * FROM inspire
    UNION ALL
    SELECT * FROM ace_hs    
  )


SELECT * FROM final
ORDER BY 1, 3, 6, 7, 8

