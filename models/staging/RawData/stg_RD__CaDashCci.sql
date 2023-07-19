with cci_2019 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        DassFlag,
        SafetyNet,
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
        CurrPrep,
        CurrPrepPct,
        CurrPrepSummative,
        CurrPrepSummativePct,
        CurrPrepApExam,
        CurrPrepApExamPct,
        CurrPrepIbExam,
        CurrPrepIbExamPct,
        CurrPrepCollegeCredit,
        CurrPrepCollegeCreditPct,
        CurrPrepAgPlus,
        CurrPrepAgPlusPct,
        CurrPrepCtePlus,
        CurrPrepCtePlusPct,
        CurrPrepSsb,
        CurrPrepSsbPct,
        CurrPrepMilSci,
        CurrPrepMilSciPct,
        CurrAPrep,
        CurrAPrepPct,
        CurrAPrepSummative,
        CurrAPrepSummativePct,
        CurrAPrepCollegeCredit,
        CurrAPrepCollegeCreditPct,
        CurrAPrepAg,
        CurrAPrepAgPct,
        CurrAPrepCte,
        CurrAPrepCtePct,
        CurrAPrepMilSci,
        CurrAPrepMilSciPct,
        CurrNPrep,
        CurrNPrepPct,
        PriorPrep,
        PriorPrepPct,
        PriorPrepSummative,
        PriorPrepSummativePct,
        PriorPrepApExam,
        PriorPrepApExamPct,
        PriorPrepIbExam,
        PriorPrepIbExamPct,
        PriorPrepCollegeCredit,
        PriorPrepCollegeCreditPct,
        PriorPrepAgPlus,
        PriorPrepAgPlusPct,
        PriorPrepCtePlus,
        PriorPrepCtePlusPct,
        PriorPrepSsb,
        PriorPrepSsbPct,
        PriorPrepMilSci,
        PriorPrepMilSciPct,
        PriorAPrep,
        PriorAPrepPct,
        PriorAPrepSummative,
        PriorAPrepSummativePct,
        PriorAPrepCollegeCredit,
        PriorAPrepCollegeCreditPct,
        PriorAPrepAg,
        PriorAPrepAgPct,
        PriorAPrepCte,
        PriorAPrepCtePct,
        PriorAPrepMilSci,
        PriorAPrepMilSciPct,
        PriorNPrep,
        PriorNPrepPct,
        ReportingYear
    from {{ ref('base_RD__CaDashCci2019')}} 
),

cci_2018 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        DassFlag,
        cast(null as bool) as SafetyNet,
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
        CurrPrep,
        CurrPrepPct,
        CurrPrepSummative,
        CurrPrepSummativePct,
        CurrPrepApExam,
        CurrPrepApExamPct,
        CurrPrepIbExam,
        CurrPrepIbExamPct,
        CurrPrepCollegeCredit,
        CurrPrepCollegeCreditPct,
        CurrPrepAgPlus,
        CurrPrepAgPlusPct,
        CurrPrepCtePlus,
        CurrPrepCtePlusPct,
        CurrPrepSsb,
        CurrPrepSsbPct,
        CurrPrepMilSci,
        CurrPrepMilSciPct,
        CurrAPrep,
        CurrAPrepPct,
        CurrAPrepSummative,
        CurrAPrepSummativePct,
        CurrAPrepCollegeCredit,
        CurrAPrepCollegeCreditPct,
        CurrAPrepAg,
        CurrAPrepAgPct,
        CurrAPrepCte,
        CurrAPrepCtePct,
        CurrAPrepMilSci,
        CurrAPrepMilSciPct,
        CurrNPrep,
        CurrNPrepPct,
        PriorPrep,
        PriorPrepPct,
        PriorPrepSummative,
        PriorPrepSummativePct,
        PriorPrepApExam,
        PriorPrepApExamPct,
        PriorPrepIbExam,
        PriorPrepIbExamPct,
        PriorPrepCollegeCredit,
        PriorPrepCollegeCreditPct,
        PriorPrepAgPlus,
        PriorPrepAgPlusPct,
        PriorPrepCtePlus,
        PriorPrepCtePlusPct,
        PriorPrepSsb,
        PriorPrepSsbPct,
        PriorPrepMilSci,
        PriorPrepMilSciPct,
        PriorAPrep,
        PriorAPrepPct,
        PriorAPrepSummative,
        PriorAPrepSummativePct,
        PriorAPrepCollegeCredit,
        PriorAPrepCollegeCreditPct,
        PriorAPrepAg,
        PriorAPrepAgPct,
        PriorAPrepCte,
        PriorAPrepCtePct,
        PriorAPrepMilSci,
        PriorAPrepMilSciPct,
        PriorNPrep,
        PriorNPrepPct,
        ReportingYear
    from {{ ref('base_RD__CaDashCci2018')}} 
),

cci_2017 as (
    select
        Cds,
        RType,
        SchoolName,
        DistrictName,
        CountyName,
        CharterFlag,
        CoeFlag,
        cast(null as bool) as DassFlag,
        cast(null as bool) as SafetyNet,
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
        CurrPrep,
        CurrPrepPct,
        cast(null as int64) CurrPrepSummative,
        cast(null as float64) as CurrPrepSummativePct,
        cast(null as int64) CurrPrepApExam,
        cast(null as float64) as CurrPrepApExamPct,
        cast(null as int64) CurrPrepIbExam,
        cast(null as float64) as CurrPrepIbExamPct,
        cast(null as int64) CurrPrepCollegeCredit,
        cast(null as float64) as CurrPrepCollegeCreditPct,
        cast(null as int64) CurrPrepAgPlus,
        cast(null as float64) as CurrPrepAgPlusPct,
        cast(null as int64) CurrPrepCtePlus,
        cast(null as float64) as CurrPrepCtePlusPct,
        cast(null as int64) CurrPrepSsb,
        cast(null as float64) as CurrPrepSsbPct,
        cast(null as int64) CurrPrepMilSci,
        cast(null as float64) as CurrPrepMilSciPct,
        cast(null as int64) CurrAPrep,
        cast(null as float64) as CurrAPrepPct,
        cast(null as int64) CurrAPrepSummative,
        cast(null as float64) as CurrAPrepSummativePct,
        cast(null as int64) CurrAPrepCollegeCredit,
        cast(null as float64) as CurrAPrepCollegeCreditPct,
        cast(null as int64) CurrAPrepAg,
        cast(null as float64) as CurrAPrepAgPct,
        cast(null as int64) CurrAPrepCte,
        cast(null as float64) as CurrAPrepCtePct,
        cast(null as int64) CurrAPrepMilSci,
        cast(null as float64) as CurrAPrepMilSciPct,
        cast(null as int64) CurrNPrep,
        cast(null as float64) as CurrNPrepPct,
        cast(null as int64) PriorPrep,
        cast(null as float64) as PriorPrepPct,
        cast(null as int64) PriorPrepSummative,
        cast(null as float64) as PriorPrepSummativePct,
        cast(null as int64) PriorPrepApExam,
        cast(null as float64) as PriorPrepApExamPct,
        cast(null as int64) PriorPrepIbExam,
        cast(null as float64) as PriorPrepIbExamPct,
        cast(null as int64) PriorPrepCollegeCredit,
        cast(null as float64) as PriorPrepCollegeCreditPct,
        cast(null as int64) PriorPrepAgPlus,
        cast(null as float64) as PriorPrepAgPlusPct,
        cast(null as int64) PriorPrepCtePlus,
        cast(null as float64) as PriorPrepCtePlusPct,
        cast(null as int64) PriorPrepSsb,
        cast(null as float64) as PriorPrepSsbPct,
        cast(null as int64) PriorPrepMilSci,
        cast(null as float64) as PriorPrepMilSciPct,
        cast(null as int64) PriorAPrep,
        cast(null as float64) as PriorAPrepPct,
        cast(null as int64) PriorAPrepSummative,
        cast(null as float64) as PriorAPrepSummativePct,
        cast(null as int64) PriorAPrepCollegeCredit,
        cast(null as float64) as PriorAPrepCollegeCreditPct,
        cast(null as int64) PriorAPrepAg,
        cast(null as float64) as PriorAPrepAgPct,
        cast(null as int64) PriorAPrepCte,
        cast(null as float64) as PriorAPrepCtePct,
        cast(null as int64) PriorAPrepMilSci,
        cast(null as float64) as PriorAPrepMilSciPct,
        cast(null as int64) PriorNPrep,
        cast(null as float64) as PriorNPrepPct,
        ReportingYear
    from {{ ref('base_RD__CaDashCci2017')}} 
),

unioned as (
    select * from cci_2019
    union all
    select * from cci_2018
    union all
    select * from cci_2017
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
    where CodeColumn = 'StatusLevel - College/Career'
),

change_levels as (
    select
        cast(Code as int64) as ChangeLevel,
        Value as ChangeLevelName
    from codes
    where CodeColumn = 'ChangeLevel - College/Career'
),

final as (
    select
        'College/Career' as IndicatorName,
        ifnull(entities.EntityType, if(unioned_w_entity_codes.Rtype = 'S', 'School', NULL)) as EntityType,
        ifnull(entities.EntityName, unioned_w_entity_codes.SchoolName) as EntityName,
        ifnull(entities.EntityNameShort, unioned_w_entity_codes.SchoolName) as EntityNameShort,
        student_groups.StudentGroupName,
        ifnull(status_levels.StatusLevelName, 'No Status Level') as StatusLevelName,
        ifnull(change_levels.ChangeLevelName, 'No Change Level') as ChangeLevelName,
        ifnull(colors.ColorName, 'No Color') as ColorName,
        unioned_w_entity_codes.*
    from unioned_w_entity_codes
    left join entities
    on unioned_w_entity_codes.EntityCode = entities.EntityCode
    left join student_groups
    on unioned_w_entity_codes.StudentGroup = student_groups.StudentGroup
    left join status_levels
    on unioned_w_entity_codes.StatusLevel = status_levels.StatusLevel
    left join change_levels
    on unioned_w_entity_codes.ChangeLevel = change_levels.ChangeLevel
    left join colors
    on unioned_w_entity_codes.Color = colors.Color
)

select * from final
