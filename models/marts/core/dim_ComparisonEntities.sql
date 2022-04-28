WITH
  ace_entities AS (
    SELECT
      StateSchoolCode AS EntityCode,
      'School' AS EntityType,
      SchoolNameFull AS EntityName,
      SchoolNameMid AS EntityNameMid,
      SchoolNameShort AS EntityNameShort,
      StateSchoolCode AS AceComparisonSchoolCode
    FROM {{ ref('stg_GSD__Schools')}}
  ),
  
  comparison_entities AS (
    SELECT
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort AS EntityNameMid,
      EntityNameShort,
      AceComparisonSchoolCode
    FROM {{ ref('stg_GSD__ComparisonEntities')}} 
  ),

  unioned AS (
    SELECT * FROM ace_entities
    UNION ALL
    SELECT * FROM comparison_entities
  ),

  final AS (
    SELECT
      u.*,
      a.EntityNameMid AS AceComparisonSchoolName
    FROM unioned AS u
    LEFT JOIN ace_entities AS a
    ON u.AceComparisonSchoolCode = a.EntityCode
  )

SELECT * FROM final
