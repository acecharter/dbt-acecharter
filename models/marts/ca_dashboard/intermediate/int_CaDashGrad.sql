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
  FROM {{ ref('stg_RD__CaDashGrad2019')}} 
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
    FROM {{ ref('stg_RD__CaDashGrad2018')}} 
  ),

  unioned AS (
    SELECT * FROM grad_2018
    UNION ALL
    SELECT * FROM grad_2019
  ),

  final AS (
    SELECT
      'Graduation Rate' AS IndicatorName,
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  )

SELECT * FROM final
