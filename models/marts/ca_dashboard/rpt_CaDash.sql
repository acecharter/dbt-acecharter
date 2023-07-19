with ela as (
    select
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
    from {{ ref('stg_RD__CaDashEla') }}
),

math as (
    select
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
    from {{ ref('stg_RD__CaDashMath') }}
),

elpi as (
    select
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
        cast(null as string) as StudentGroup,
        cast(null as string) as StudentGroupName,
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
    from {{ ref('stg_RD__CaDashElpi') }}
),

cci as (
    select
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
    from {{ ref('stg_RD__CaDashCci') }}
),

chronic as (
    select
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
    from {{ ref('stg_RD__CaDashChronic') }}
),

susp as (
    select
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
    from {{ ref('stg_RD__CaDashSusp') }}
),

grad as (
    select
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
    from {{ ref('stg_RD__CaDashGrad') }}
),

unioned as (
    select * from cci
    union all
    select * from chronic
    union all
    select * from ela
    union all
    select * from elpi
    union all
    select * from grad
    union all
    select * from math
    union all
    select * from susp
)

select * from unioned
