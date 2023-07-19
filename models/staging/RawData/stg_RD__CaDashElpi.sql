with elpi_2022 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        DassFlag,
        CurrProgressed,
        PctCurrProgressed,
        CurrMaintainPL4,
        PctCurrMaintainPL4,
        CurrMaintainOth,
        PctCurrMaintainOth,
        CurrDeclined,
        CurrNumer,
        CurrDenom,
        CurrStatus,
        null as PriorDenom,
        null as PriorStatus,
        null as Change,
        StatusLevel,
        null as ChangeLevel,
        null as Color,
        null as Box,
        Flag95Pct,
        NSizeMet,
        NSizeGroup,
        ReportingYear
    from {{ ref('base_RD__CaDashElpi2022')}} 
),

elpi_2019 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        DassFlag,
        CurrProgressed,
        cast(null as float64) as PctCurrProgressed,
        CurrMaintainPL4,
        cast(null as float64) as PctCurrMaintainPL4,
        cast(null as int64) as CurrMaintainOth,
        cast(null as float64) as PctCurrMaintainOth,
        CurrDeclined,
        CurrNumer,
        CurrDenom,
        CurrStatus,
        null as PriorDenom,
        null as PriorStatus,
        null as Change,
        StatusLevel,
        null as ChangeLevel,
        null as Color,
        null as Box,
        Flag95Pct,
        NSizeMet,
        NSizeGroup,
        ReportingYear
    from {{ ref('base_RD__CaDashElpi2019')}} 
),

elpi_2017 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        cast(null as bool) as DassFlag,
        CurrProgressed,
        cast(null as float64) as PctCurrProgressed,
        CurrMaintainPL4,
        cast(null as float64) as PctCurrMaintainPL4,
        cast(null as int64) as CurrMaintainOth,
        cast(null as float64) as PctCurrMaintainOth,
        cast(null as int64) as CurrDeclined,
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
        cast(null as bool) as Flag95Pct,
        NSizeMet,
        cast(null as string) as NSizeGroup,
        ReportingYear
    from {{ ref('base_RD__CaDashElpi2017')}} 
),

unioned as (
    select * from elpi_2022
    union all
    select * from elpi_2019
    union all
    select * from elpi_2017
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
    where CodeColumn = 'StatusLevel - EL Progress'
),

change_levels as (
    select
        cast(Code as int64) as ChangeLevel,
        Value as ChangeLevelName
    from codes
    where CodeColumn = 'ChangeLevel - EL Progress'
),

final as (
    select
        'EL Progress' as IndicatorName,
        ifnull(e.EntityType, if(u.Rtype = 'S', 'School', null)) as EntityType,
        ifnull(e.EntityName, u.SchoolName) as EntityName,
        ifnull(e.EntityNameShort, u.SchoolName) as EntityNameShort,
        ifnull(sl.StatusLevelName, 'No Status Level') as StatusLevelName,
        ifnull(cl.ChangeLevelName, 'No Change Level') as ChangeLevelName,
        ifnull(c.ColorName, 'No Color') as ColorName,
        u.*
    from unioned_w_entity_codes as u
    left join entities as e
    on u.EntityCode = e.EntityCode
    left join status_levels as sl
    on u.StatusLevel = sl.StatusLevel
    left join change_levels as cl
    on u.ChangeLevel = cl.ChangeLevel
    left join colors as c
    on u.Color = c.Color
)

select * from final
