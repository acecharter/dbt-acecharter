WITH 
  chronic_2022 AS (
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
      CurrNumer,
      CurrDenom,
      CurrStatus,
      CAST(NULL AS INT64) AS PriorNumer,
      CAST(NULL AS INT64) AS PriorDenom,
      CAST(NULL AS FLOAT64) AS PriorStatus,
      CAST(NULL AS FLOAT64) AS Change,
      CAST(NULL AS BOOL) AS SafetyNet,
      StatusLevel,
      CAST(NULL AS INT64) AS ChangeLevel,
      CAST(NULL AS INT64) AS Color,
      CAST(NULL AS INT64) AS Box,
      CertifyFlag,
      DataErrorFlag,
      ReportingYear
  FROM {{ ref('base_RD__CaDashChronic2022')}} 
  ),

  chronic_2019 AS (
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
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorNumer,
      PriorDenom,
      PriorStatus,
      Change,
      SafetyNet,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CertifyFlag,
      DataErrorFlag,
      ReportingYear
  FROM {{ ref('base_RD__CaDashChronic2019')}} 
  ),
  
  chronic_2018 AS (
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
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorNumer,
      PriorDenom,
      PriorStatus,
      Change,
      SafetyNet,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CertifyFlag,
      CAST(NULL AS BOOL) AS DataErrorFlag,
      ReportingYear
    FROM {{ ref('base_RD__CaDashChronic2018')}} 
  ),

  unioned AS (
    SELECT * FROM chronic_2022
    UNION ALL
    SELECT * FROM chronic_2019
    UNION ALL
    SELECT * FROM chronic_2018
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
    WHERE CodeColumn = 'StatusLevel - Chronic Absenteeism'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - Chronic Absenteeism'
  ),

  final AS (
    SELECT
      'Chronic Absenteeism' AS IndicatorName,
      IFNULL(e.EntityType, IF(u.Rtype = 'S', 'School', NULL)) AS EntityType,
      IFNULL(e.EntityName, u.SchoolName) AS EntityName,
      IFNULL(e.EntityNameShort, u.SchoolName) AS EntityNameShort,
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
