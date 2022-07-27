WITH 
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

  unioned AS (
    SELECT * FROM susp_2018
    UNION ALL
    SELECT * FROM susp_2019
  ),

  final AS (
    SELECT
      'Suspension Rate' AS IndicatorName,
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  )

SELECT * FROM final
