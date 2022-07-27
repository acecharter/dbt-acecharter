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

  unioned AS (
    SELECT * FROM ela_2018
    UNION ALL
    SELECT * FROM ela_2019
  ),

  final AS (
    SELECT
      'ELA' AS IndicatorName,
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  )

SELECT * FROM final
