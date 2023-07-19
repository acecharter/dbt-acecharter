with entities as (
    select * from {{ ref('dim_ComparisonEntities') }}
),

entity_info as (
    select distinct * except (AceComparisonSchoolCode, AceComparisonSchoolName)
    from entities
),

enrollment as (
    select
        e.SchoolYear,
        i.EntityType,
        e.EntityCode,
        i.EntityName,
        i.EntityNameShort,
        e.SchoolType,
        e.SubgroupType,
        e.Subgroup,
        e.Gender,
        e.GradeLevel,
        e.Enrollment,
        e.PctOfTotalEnrollment
    from {{ ref('fct_CdeEnrollment') }} as e
    left join entity_info as i
        on e.EntityCode = i.EntityCode
    where
        i.EntityCode is not null
        and e.Enrollment is not null
),

empower as (
    select
        e.AceComparisonSchoolName,
        e.AceComparisonSchoolCode,
        enr.*
    from entities as e
    left join enrollment as enr
        on e.EntityCode = enr.EntityCode
    where
        e.AceComparisonSchoolCode = '0116814'
        and GradeLevel in ('5', '6', '7', '8', 'All')
),

esperanza as (
    select
        e.AceComparisonSchoolName,
        e.AceComparisonSchoolCode,
        enr.*
    from entities as e
    left join enrollment as enr
        on e.EntityCode = enr.EntityCode
    where
        e.AceComparisonSchoolCode = '0129247'
        and GradeLevel in ('5', '6', '7', '8', 'All')
),

inspire as (
    select
        e.AceComparisonSchoolName,
        e.AceComparisonSchoolCode,
        enr.*
    from entities as e
    left join enrollment as enr
        on e.EntityCode = enr.EntityCode
    where
        e.AceComparisonSchoolCode = '0131656'
        and GradeLevel in ('5', '6', '7', '8', 'All')
),

ace_hs as (
    select
        e.AceComparisonSchoolName,
        e.AceComparisonSchoolCode,
        enr.*
    from entities as e
    left join enrollment as enr
        on e.EntityCode = enr.EntityCode
    where
        e.AceComparisonSchoolCode = '0125617'
        and GradeLevel in ('9', '10', '11', '12', 'All')
),

final as (
    select * from empower
    union all
    select * from esperanza
    union all
    select * from inspire
    union all
    select * from ace_hs
)


select * from final
order by 1, 3, 6, 7, 8
