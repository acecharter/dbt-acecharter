with ela_2022 as (
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
        CurrDenom,
        CurrStatus,
        cast(null as int64) as PriorDenom,
        cast(null as float64) as PriorStatus,
        cast(null as float64) as Change,
        StatusLevel,
        cast(null as int64) as ChangeLevel,
        cast(null as int64) as Color,
        cast(null as int64) as Box,
        HsCutPoints,
        cast(null as float64) as CurrAdjustment,
        cast(null as float64) as PriorAdjustment,
        PairShareMethod,
        cast(null as bool) as NoTestFlag,
        PRateEnrolled,
        PRateTested,
        PRate,
        NumPrLoss,
        CurrDenomWithoutPrLoss,
        CurrStatusWithoutPrLoss,
        ReportingYear
    from {{ ref('base_RD__CaDashEla2022')}} 
),

ela_2019 as (
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
        CurrDenom,
        CurrStatus,
        PriorDenom,
        PriorStatus,
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        HsCutPoints,
        CurrAdjustment,
        PriorAdjustment,
        PairShareMethod,
        NoTestFlag,
        cast(null as int64) as PRateEnrolled,
        cast(null as int64) as PRateTested,
        cast(null as int64) as PRate,
        cast(null as int64) as NumPrLoss,
        cast(null as int64) as CurrDenomWithoutPrLoss,
        cast(null as float64) as CurrStatusWithoutPrLoss,
        ReportingYear
    from {{ ref('base_RD__CaDashEla2019')}} 
),

ela_2018 as (
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
        CurrDenom,
        CurrStatus,
        PriorDenom,
        PriorStatus,
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        Box,
        HsCutPoints,
        CurrAdjustment,
        PriorAdjustment,
        PairShareMethod,
        cast(null as bool) as NoTestFlag,
        cast(null as int64) as PRateEnrolled,
        cast(null as int64) as PRateTested,
        cast(null as int64) as PRate,
        cast(null as int64) as NumPrLoss,
        cast(null as int64) as CurrDenomWithoutPrLoss,
        cast(null as float64) as CurrStatusWithoutPrLoss,
        ReportingYear
    from {{ ref('base_RD__CaDashEla2018')}} 
),

ela_2017 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        cast(null as bool) as DassFlag,
        StudentGroup,
        CurrDenom,
        CurrStatus,
        PriorDenom,
        PriorStatus,
        Change,
        StatusLevel,
        ChangeLevel,
        Color,
        cast(null as int64) as Box,
        cast(null as bool) as HsCutPoints,
        cast(null as float64) as CurrAdjustment,
        cast(null as float64) as PriorAdjustment,
        cast(null as string) as PairShareMethod,
        cast(null as bool) as NoTestFlag,
        cast(null as int64) as PRateEnrolled,
        cast(null as int64) as PRateTested,
        cast(null as int64) as PRate,
        cast(null as int64) as NumPrLoss,
        cast(null as int64) as CurrDenomWithoutPrLoss,
        cast(null as float64) as CurrStatusWithoutPrLoss,
        ReportingYear
    from {{ ref('base_RD__CaDashEla2017')}} 
),

unioned as (
    select * from ela_2022
    union all
    select * from ela_2019
    union all
    select * from ela_2018
    union all
    select * from ela_2017
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
    where CodeColumn = 'StatusLevel - ELA'
),

change_levels as (
    select
        cast(Code as int64) as ChangeLevel,
        Value as ChangeLevelName
    from codes
    where CodeColumn = 'ChangeLevel - ELA'
),

final as (
    select
        'ELA' as IndicatorName,
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
