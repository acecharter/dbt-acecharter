with entity_enr as (
    select * from {{ ref('stg_GSD__CdeEnrBySubgroupEntities') }}
),

entity_enr_charter as (
    select
        SchoolYear,
        EntityCode,
        Subgroup,
        'Charter Schools' as EnrollmentType,
        CharterSchoolEnrollment as Enrollment
    from entity_enr
),

entity_enr_non_charter as (
    select
        SchoolYear,
        EntityCode,
        Subgroup,
        'Non-Charter Schools' as EnrollmentType,
        NonCharterSchoolEnrollment as Enrollment
    from entity_enr
),

entity_enr_total as (
    select
        SchoolYear,
        EntityCode,
        Subgroup,
        'All Schools' as EnrollmentType,
        TotalEnrollment as Enrollment
    from entity_enr
),

entity_enr_unioned as (
    select * from entity_enr_charter
    union all
    select * from entity_enr_non_charter
    union all
    select * from entity_enr_total
),

entity_enr_all_students as (
    select *
    from entity_enr_unioned
    where Subgroup = 'All Students'
),

entity_enr_final as (
    select
        u.*,
        round(u.Enrollment / a.Enrollment, 4) as PctOfTotalEnrollment
    from entity_enr_unioned as u
    left join entity_enr_all_students as a
        on
            u.SchoolYear = a.SchoolYear
            and u.EntityCode = a.EntityCode
            and u.EnrollmentType = a.EnrollmentType
),

school_enr as (
    select * from {{ ref('stg_GSD__CdeEnrBySubgroupSchools') }}
),

school_enr_all_students as (
    select *
    from school_enr
    where Subgroup = 'All Students'
),

school_enr_final as (
    select
        s.SchoolYear,
        s.SchoolCode as EntityCode,
        s.Subgroup,
        'All Schools' as EnrollmentType,
        s.Enrollment,
        round(s.Enrollment / a.Enrollment, 4) as PctOfTotalEnrollment
    from school_enr as s
    left join school_enr_all_students as a
        on
            s.SchoolYear = a.SchoolYear
            and s.SchoolCode = a.SchoolCode
),

final as (
    select * from entity_enr_final
    union all
    select * from school_enr_final
)

select * from final
