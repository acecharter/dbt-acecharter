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
      NULL AS Box,
      Flag95Pct,
      NSizeMet,
      NSizeGroup,
      ReportingYear
    FROM {{ ref('base_RD__CaDashElpi2019')}} 
  ),

  elpi_2017 AS (
    SELECT
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      CAST(NULL AS BOOL) AS DassFlag,
      CurrProgressed,
      CurrMaintainPL4,
      CAST(NULL AS INT64) AS CurrDeclined,
      CurrNumer,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      Box,
      CAST(NULL AS BOOL) AS Flag95Pct,
      NSizeMet,
      CAST(NULL AS STRING) AS NSizeGroup,
      ReportingYear
    FROM {{ ref('base_RD__CaDashElpi2017')}} 
  ),

  unioned AS (
    SELECT * FROM elpi_2019
    UNION ALL
    SELECT * FROM elpi_2017
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

  status_levels AS (
    SELECT
      CAST(Code AS INT64) AS StatusLevel,
      Value AS StatusLevelName
    FROM codes
    WHERE CodeColumn = 'StatusLevel - EL Progress'
  ),

  change_levels AS (
    SELECT
      CAST(Code AS INT64) AS ChangeLevel,
      Value AS ChangeLevelName
    FROM codes
    WHERE CodeColumn = 'ChangeLevel - EL Progress'
  ),

  final AS (
    SELECT
      'EL Progress' AS IndicatorName,
      IFNULL(e.EntityType, IF(u.Rtype = 'S', 'School', NULL)) AS EntityType,
      IFNULL(e.EntityName, u.SchoolName) AS EntityName,
      IFNULL(e.EntityNameShort, u.SchoolName) AS EntityNameShort,
      IFNULL(sl.StatusLevelName, 'No Status Level') AS StatusLevelName,
      IFNULL(cl.ChangeLevelName, 'No Change Level') AS ChangeLevelName,
      IFNULL(c.ColorName, 'No Color') AS ColorName,
      u.*
    FROM unioned_w_entity_codes AS u
    LEFT JOIN entities AS e
    ON u.EntityCode = e.EntityCode
    LEFT JOIN status_levels AS sl
    ON u.StatusLevel = sl.StatusLevel
    LEFT JOIN change_levels AS cl
    ON u.StatusLevel = cl.ChangeLevel
    LEFT JOIN colors AS c
    ON u.Color = c.Color
  )

SELECT * FROM final
