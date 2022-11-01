WITH 
  ela AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashEla')}} 
  ),

  math AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashMath')}} 
  ),

  elpi AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      CAST(NULL AS STRING) AS StudentGroup,
      CAST(NULL AS STRING) AS StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashElpi')}}
  ),

  cci AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashCci')}}
  ),

  chronic AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashChronic')}}
  ),

  susp AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashSusp')}}
  ),

  grad AS (
    SELECT
      IndicatorName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameShort,
      Cds,
      RType,
      SchoolName,
      DistrictName,
      CountyName,
      CharterFlag,
      CoeFlag,
      DassFlag,
      StudentGroup,
      StudentGroupName,
      CurrDenom,
      CurrStatus,
      PriorDenom,
      PriorStatus,
      Change,
      StatusLevel,
      ChangeLevel,
      Color,
      StatusLevelName,
      ChangeLevelName,
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('stg_RD__CaDashGrad')}}
  ),

  unioned AS (
    SELECT * FROM cci
    UNION ALL
    SELECT * FROM chronic
    UNION ALL
    SELECT * FROM ela
    UNION ALL
    SELECT * FROM elpi
    UNION ALL
    SELECT * FROM grad
    UNION ALL
    SELECT * FROM math
    UNION ALL
    SELECT * FROM susp
  )

SELECT * FROM unioned