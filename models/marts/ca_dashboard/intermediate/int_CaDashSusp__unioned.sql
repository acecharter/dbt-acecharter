WITH 
  susp_2018 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      Type,
      StudentGroup,
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorNumer,
      PriorDenom,
      PriorStatus,
      SafetyNet,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CertifyFlag,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashSusp2018')}} 
  ),

  susp_2019 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      Type,
      StudentGroup,
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorNumer,
      PriorDenom,
      PriorStatus,
      SafetyNet,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CertifyFlag,
      ReportingYear
  FROM {{ ref('stg_RD__CaDashSusp2019')}} 
  ),

  unioned AS (
    SELECT * FROM susp_2018
    UNION ALL
    SELECT * FROM susp_2019
  )

SELECT * FROM unioned
