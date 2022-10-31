WITH 
  ela_2019 AS (
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
  FROM {{ ref('stg_RD__CaDashEla2019')}} 
  ),
  
  ela_2018 AS (
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
    FROM {{ ref('stg_RD__CaDashEla2018')}} 
  ),
  
  ela_2017 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CAST(CoeFlag AS BOOL) AS CoeFlag,
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
      CAST(ReportingYear
    FROM {{ ref('stg_RD__CaDashEla2017')}} 
  ),

  unioned AS (
    SELECT * FROM ela_2019
    UNION ALL
    SELECT * FROM ela_2018
    UNION ALL
    SELECT * FROM ela_2017
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
    WHERE CodeColumn = 'StatusLevel - ELA'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - ELA'
  ),

  final AS (
    SELECT
      'ELA' AS IndicatorName,
      e.EntityType,
      e.EntityName,
      e.EntityNameShort,
      g.StudentGroupName,
      sl.StatusLevelName,
      cl.ChangeLevelName,
      c.ColorName,
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

