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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashEla')}} 
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashMath')}} 
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashElpi')}}
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashCci')}}
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashChronic')}}
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashSusp')}}
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
      ColorName,
      Box,
      ReportingYear
    FROM {{ ref('int_CaDashGrad')}}
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