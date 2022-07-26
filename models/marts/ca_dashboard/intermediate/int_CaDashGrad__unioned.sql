WITH 
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
    FROM {{ ref('stg_RD__CaDashGrad2018')}} 
  ),

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
  FROM {{ ref('stg_RD__CaDashGrad2019')}} 
  ),

  unioned AS (
    SELECT * FROM grad_2018
    UNION ALL
    SELECT * FROM grad_2019
  )

SELECT * FROM unioned
