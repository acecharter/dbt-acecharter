WITH 
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
  FROM {{ ref('stg_RD__CaDashChronic2019')}} 
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
    FROM {{ ref('stg_RD__CaDashChronic2018')}} 
  ),

  unioned AS (
    SELECT * FROM chronic_2018
    UNION ALL
    SELECT * FROM chronic_2019
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

  final AS (
    SELECT
      'Chronic Absenteeism' AS IndicatorName,
      e.EntityType,
      e.EntityName,
      e.EntityNameShort,
      g.StudentGroupName,
      c.ColorName,
      u.*
    FROM unioned_w_entity_codes AS u
    LEFT JOIN entities AS e
    ON u.EntityCode = e.EntityCode
    LEFT JOIN student_groups AS g
    ON u.StudentGroup = g.StudentGroup
    LEFT JOIN colors AS c
    ON u.Color = c.Color
  )

SELECT * FROM final
