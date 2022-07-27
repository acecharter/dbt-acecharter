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

  final AS (
    SELECT
      'Chronic Absenteeism' AS IndicatorName,
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  )

SELECT * FROM final
