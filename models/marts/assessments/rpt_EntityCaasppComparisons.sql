WITH
  entities AS (
    SELECT * FROM {{ ref('dim_ComparisonEntities')}}
  ),

  caaspp AS (
    SELECT * FROM {{ ref('fct_EntityCaaspp')}}
  ),

  empower AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      c.*
    FROM entities AS e
    LEFT JOIN caaspp AS c
    ON e.EntityCode = c.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0116814'
      AND GradeLevel IN (5, 6, 7, 8, 13)
  ),

  esperanza AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      c.*
    FROM entities AS e
    LEFT JOIN caaspp AS c
    ON e.EntityCode = c.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0129247'
      AND GradeLevel IN (5, 6, 7, 8, 13)
  ),

  inspire AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      c.*
    FROM entities AS e
    LEFT JOIN caaspp AS c
    ON e.EntityCode = c.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0131656'
      AND GradeLevel IN (5, 6, 7, 8, 13)
  ),

  ace_hs AS (
    SELECT
      e.AceComparisonSchoolName,
      e.AceComparisonSchoolCode,
      c.*
    FROM entities AS e
    LEFT JOIN caaspp AS c
    ON e.EntityCode = c.EntityCode
    WHERE
      e.AceComparisonSchoolCode = '0125617'
      AND GradeLevel IN (11, 13)
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
