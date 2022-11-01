WITH 
  grad_2019 AS (
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
      FiveYrNumer,
      SafetyNet,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      ReportingYear
  FROM {{ ref('base_RD__CaDashGrad2019')}} 
  ),

  grad_2018 AS (
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
      NULL AS FiveYrNumer,
      SafetyNet,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      ReportingYear
    FROM {{ ref('base_RD__CaDashGrad2018')}} 
  ),

  grad_2017 AS (
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
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorNumer,
      PriorDenom,
      PriorStatus,
      NULL AS FiveYrNumer,
      SafetyNet,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      ReportingYear
    FROM {{ ref('base_RD__CaDashGrad2017')}} 
  ),

  unioned AS (
    SELECT * FROM grad_2019
    UNION ALL
    SELECT * FROM grad_2018
    UNION ALL
    SELECT * FROM grad_2017
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
    WHERE CodeColumn = 'StatusLevel - Graduation Rate'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - Graduation Rate'
  ),

  final AS (
    SELECT
      'Graduation Rate' AS IndicatorName,
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
