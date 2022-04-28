WITH
  ace_entities AS (
    SELECT
      StateSchoolCode AS EntityCode,
      'School' AS EntityType,
      SchoolNameFull AS EntityName,
      SchoolNameMid AS EntityNameMid,
      SchoolNameShort AS EntityNameShort
    FROM {{ ref('stg_GSD__Schools')}}
  ),
  
  comparison_entities AS (
    SELECT DISTINCT
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort AS EntityNameMid,
      EntityNameShort
    FROM {{ ref('stg_GSD__ComparisonEntities')}} 
  ),

  final AS (
    SELECT * FROM ace_entities
    UNION ALL
    SELECT * FROM comparison_entities
  )

SELECT * FROM final
