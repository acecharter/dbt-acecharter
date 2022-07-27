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
      NULL AS PriorDenom,
      NULL AS PriorStatus,
      NULL AS Change,
      StatusLevel,
      NULL AS ChangeLevel,
      NULL AS Color,
      ColorName,
      NULL AS Box,
      Flag95Pct,
      NSizeMet,
      NSizeGroup,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashElpi2019')}} 
  ),

  unioned AS (
    SELECT * FROM elpi_2019
  ),

  unioned_w_entity_codes AS (
    SELECT
      CASE
        WHEN RType = 'X' THEN '00'
        WHEN RType = 'D' THEN SUBSTR(cds, 3, 5)
        WHEN RType = 'S' THEN SUBSTR(cds, LENGTH(cds)-6, 7)
      END AS EntityCode,
      *
    FROM unioned
  ),
  
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  codes AS (
    SELECT * FROM {{ ref('stg_GSD__CaDashCodes')}}
  ),

  colors AS (
    SELECT
      CAST(Code AS INT64) AS Color,
      Value AS ColorName
    FROM codes
    WHERE CodeColumn = 'Color'
  ),

  final AS (
    SELECT
      'EL Progress' AS IndicatorName,
      e.EntityType,
      e.EntityName,
      e.EntityNameShort,
      c.ColorName,
      u.*
    FROM unioned_w_entity_codes AS u
    LEFT JOIN entities AS e
    ON u.EntityCode = e.EntityCode
    LEFT JOIN colors AS c
    ON u.Color = c.Color
  )

SELECT * FROM final
