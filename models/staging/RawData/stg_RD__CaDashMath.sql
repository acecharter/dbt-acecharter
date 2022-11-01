WITH 
  math_2019 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      HsCutPoints,
      CurrAdjustment,
      PriorAdjustment,
      PairShareMethod,
      NoTestFlag,
      ReportingYear
  FROM {{ ref('base_RD__CaDashMath2019')}} 
  ),
  
  math_2018 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      HsCutPoints,
      CurrAdjustment,
      PriorAdjustment,
      PairShareMethod,
      CAST(NULL AS BOOL) AS NoTestFlag,
      ReportingYear
    FROM {{ ref('base_RD__CaDashMath2018')}} 
  ),
  
  math_2017 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      CAST(NULL AS BOOL) AS DassFlag,
      StudentGroup,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      CAST(NULL AS INT64) AS Box,
      CAST(NULL AS BOOL) AS HsCutPoints,
      CAST(NULL AS FLOAT64) AS CurrAdjustment,
      CAST(NULL AS FLOAT64) AS PriorAdjustment,
      CAST(NULL AS STRING) AS PairShareMethod,
      CAST(NULL AS BOOL) AS NoTestFlag,
      ReportingYear
    FROM {{ ref('base_RD__CaDashMath2017')}} 
  ),

  unioned AS (
    SELECT * FROM math_2019
    UNION ALL
    SELECT * FROM math_2018
    UNION ALL
    SELECT * FROM math_2017
  ),

  unioned_w_entity_codes AS (
    SELECT
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  ),
  
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  codes AS (
    SELECT * FROM {{ ref('stg_GSD__CaDashCodes')}}
  ),

  student_groups AS (
    SELECT
      Code AS StudentGroup,
      Value AS StudentGroupName
    FROM codes
    WHERE CodeColumn = 'StudentGroup'
  ),

  colors AS (
    SELECT
      CAST(Code AS INT64) AS Color,
      Value AS ColorName
    FROM codes
    WHERE CodeColumn = 'Color'
  ),

  status_levels AS (
    SELECT
      CAST(Code AS INT64) AS StatusLevel,
      Value AS StatusLevelName
    FROM codes
    WHERE CodeColumn = 'StatusLevel - Math'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - Math'
  ),

  final AS (
    SELECT
      'Math' AS IndicatorName,
      CASE
        WHEN e.EntityType IS NOT NULL THEN e.EntityType
        WHEN u.Rtype = 'S' THEN 'School'
      END AS EntityType,
      CASE
        WHEN e.EntityName IS NOT NULL THEN e.EntityName
        WHEN u.Rtype = 'S' THEN u.SchoolName
      END AS EntityName,
      CASE
        WHEN e.EntityNameShort IS NOT NULL THEN e.EntityNameShort
        WHEN u.Rtype = 'S' THEN u.SchoolName
      END AS EntityNameShort,
      g.StudentGroupName,
      IFNULL(sl.StatusLevelName, 'No Status Level') AS StatusLevelName,
      IFNULL(cl.ChangeLevelName, 'No Change Level') AS ChangeLevelName,
      IFNULL(c.ColorName, 'No Color') AS ColorName,
      u.*
    FROM unioned_w_entity_codes AS u
    LEFT JOIN entities AS e
    ON u.EntityCode = e.EntityCode
    LEFT JOIN student_groups AS g
    ON u.StudentGroup = g.StudentGroup
    LEFT JOIN status_levels AS sl
    ON u.StatusLevel = sl.StatusLevel
    LEFT JOIN change_levels AS cl
    ON u.StatusLevel = cl.ChangeLevel
    LEFT JOIN colors AS c
    ON u.Color = c.Color
  )

SELECT * FROM final
