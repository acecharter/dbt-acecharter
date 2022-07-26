WITH 
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

  unioned AS (
    SELECT * FROM chronic_2018
    UNION ALL
    SELECT * FROM chronic_2019
  )

SELECT * FROM unioned
