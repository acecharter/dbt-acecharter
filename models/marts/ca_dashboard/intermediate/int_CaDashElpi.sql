WITH 
  elpi_2019 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      CurrProgressed,
      CurrMaintainPL4,
      CurrDeclined,
      CurrNumer,
      CurrDenom,
      CurrStatus,
      StatusLevel,
      Flag95Pct,
      NSizeMet,
      NSizeGroup,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashElpi2019')}} 
  ),

  unioned AS (
    SELECT * FROM elpi_2019
  ),

  final AS (
    SELECT
      'EL Progress' AS IndicatorName,
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  )

SELECT * FROM final
