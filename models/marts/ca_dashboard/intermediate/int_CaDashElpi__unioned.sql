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
  )

SELECT * FROM unioned
