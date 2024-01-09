with susp_2023 as (
    select
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
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        SmallDenom,
        CurrNSizeMet,
        PriorNSizeMet,
        AccountabilityMet,
        CertifyFlag,
        PriorCertifyFlag,
        Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashSusp2023')}} 
),

susp_2022 as (
    select
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
        cast(null as int64) as PriorNumer,
        cast(null as int64) as PriorDenom,
        cast(null as float64) as PriorStatus,
        cast(null as float64) as Change,
        StatusLevel,
        cast(null as int64) as ChangeLevel,
        cast(null as int64) as Color,
        cast(null as int64) as Box,
        cast(null as bool) as SmallDenom,
        cast(null as bool) as CurrNSizeMet,
        cast(null as bool) as PriorNSizeMet,
        cast(null as bool) as AccountabilityMet,
        CertifyFlag,
        cast(null as bool) as PriorCertifyFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashSusp2022')}} 
),

susp_2019 as (
    select
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
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        cast(null as bool) as SmallDenom,
        cast(null as bool) as CurrNSizeMet,
        cast(null as bool) as PriorNSizeMet,
        cast(null as bool) as AccountabilityMet,
        CertifyFlag,
        cast(null as bool) as PriorCertifyFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashSusp2019')}} 
),

susp_2018 as (
    select
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
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        cast(null as bool) as SmallDenom,
        cast(null as bool) as CurrNSizeMet,
        cast(null as bool) as PriorNSizeMet,
        cast(null as bool) as AccountabilityMet,
        CertifyFlag,
        cast(null as bool) as PriorCertifyFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashSusp2018')}} 
),

susp_2017 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        cast(null as bool) as DassFlag,
        Type,
        StudentGroup,
        CurrNumer,
        CurrDenom,
        CurrStatus,
        PriorNumer,
        PriorDenom,
        PriorStatus,
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        cast(null as bool) as SmallDenom,
        cast(null as bool) as CurrNSizeMet,
        cast(null as bool) as PriorNSizeMet,
        cast(null as bool) as AccountabilityMet,
        CertifyFlag,
        cast(null as bool) as PriorCertifyFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashSusp2017')}} 
),

unioned as (
    select * from susp_2023
    union all
    select * from susp_2022
    union all
    select * from susp_2019
    union all
    select * from susp_2018
    union all
    select * from susp_2017
),

unioned_w_entity_codes as (
    select
        case
            when RType = 'X' then '00'
            when RType = 'D' then substr(cds, 3, 5)
            when RType = 'S' then substr(cds, length(cds)-6, 7)
        end as EntityCode,
        *
    from unioned
),

entities as (
    select * from {{ ref('dim_Entities')}}
),

codes as (
    select * from {{ ref('stg_GSD__CaDashCodes')}}
),

student_groups as (
    select
        Code as StudentGroup,
        Value as StudentGroupName
    from codes
    where CodeColumn = 'StudentGroup'
),

colors as (
    select
        cast(Code as int64) as Color,
        Value as ColorName
    from codes
    where CodeColumn = 'Color'
),

status_levels as (
    select
        cast(Code as int64) as StatusLevel,
        Value as StatusLevelName
    from codes
    where CodeColumn = 'StatusLevel - Suspension Rate'
),

change_levels as (
    select
        cast(Code as int64) as ChangeLevel,
        Value as ChangeLevelName
    from codes
    where CodeColumn = 'ChangeLevel - Suspension Rate'
),

final as (
    select
        'Suspension Rate' as IndicatorName,
        ifnull(e.EntityType, IF(u.Rtype = 'S', 'School', null)) as EntityType,
        ifnull(e.EntityName, u.SchoolName) as EntityName,
        ifnull(e.EntityNameShort, u.SchoolName) as EntityNameShort,
        g.StudentGroupName,
        ifnull(sl.StatusLevelName, 'No Status Level') as StatusLevelName,
        ifnull(cl.ChangeLevelName, 'No Change Level') as ChangeLevelName,
        ifnull(c.ColorName, 'No Color') as ColorName,
        u.*
    from unioned_w_entity_codes as u
    left join entities as e
    on u.EntityCode = e.EntityCode
    left join student_groups as g
    on u.StudentGroup = g.StudentGroup
    left join status_levels as sl
    on u.StatusLevel = sl.StatusLevel
    left join change_levels as cl
    on u.ChangeLevel = cl.ChangeLevel
    left join colors as c
    on u.Color = c.Color
)

select * from final
