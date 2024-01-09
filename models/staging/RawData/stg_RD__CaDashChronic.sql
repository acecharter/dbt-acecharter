with chronic_2023 as (
    select
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
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        CertifyFlag,
        DataErrorFlag,
        Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashChronic2023')}} 
),

chronic_2022 as (
    select
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
        cast(null as int64) as PriorNumer,
        cast(null as int64) as PriorDenom,
        cast(null as float64) as PriorStatus,
        cast(null as float64) as Change,
        StatusLevel,
        cast(null as int64) as ChangeLevel,
        cast(null as int64) as Color,
        cast(null as int64) as Box,
        CertifyFlag,
        DataErrorFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashChronic2022')}} 
),

chronic_2019 as (
    select
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
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        CertifyFlag,
        DataErrorFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashChronic2019')}} 
),

chronic_2018 as (
    select
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
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        CertifyFlag,
        cast(null as bool) as DataErrorFlag,
        cast(null as string) as Indicator,
        ReportingYear
    from {{ ref('base_RD__CaDashChronic2018')}} 
),

unioned as (
    select * from chronic_2023
    union all
    select * from chronic_2022
    union all
    select * from chronic_2019
    union all
    select * from chronic_2018
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
    where CodeColumn = 'StatusLevel - Chronic Absenteeism'
),

change_levels as (
    select
        cast(Code as int64) as ChangeLevel,
        Value as ChangeLevelName
    from codes
    where CodeColumn = 'ChangeLevel - Chronic Absenteeism'
),

final as (
    select
        'Chronic Absenteeism' as IndicatorName,
        ifnull(e.EntityType, if(u.Rtype = 'S', 'School', null)) as EntityType,
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
