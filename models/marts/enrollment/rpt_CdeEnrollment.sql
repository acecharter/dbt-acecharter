with entities as (
    select * from {{ ref('dim_Entities') }}
),

enrollment as (
    select * from {{ ref('fct_CdeEnrollment') }}
),

final as (
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
    from enrollment as e
    left join entities as i
        on e.EntityCode = i.EntityCode
    where
        i.EntityCode is not null
        and e.Enrollment is not null
)

select * from final

order by 1, 3, 6, 7, 8
