WITH 
  math_2018 AS (
    SELECT
      Cds,
      Rtype,
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

  math_2019 AS (
    SELECT
      Cds,
      Rtype,
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

  unioned AS (
    SELECT * FROM math_2018
    UNION ALL
    SELECT * FROM math_2019
  )

SELECT * FROM unioned
