with race_and_grade as (
    select
        SchoolYear,
        EntityCode,
        SchoolType,
        'Race/Ethnicity' as SubgroupType,
        RaceEthnicity as Subgroup,
        Gender,
        GradeLevel,
        Enrollment,
        PctOfTotalEnrollment
    from {{ ref('int_CdeEnrByRaceAndGrade__3_unpivoted') }}
),

subgroups as (
    select
        SchoolYear,
        EntityCode,
        EnrollmentType as SchoolType,
        'Other' as SubgroupType,
        Subgroup,
        'All' as Gender,
        'All' as GradeLevel,
        Enrollment,
        PctOfTotalEnrollment
    from {{ ref('int_CdeEnrBySubgroup__transformed_unioned') }}
),

final as (
    select * from race_and_grade
    union all
    select * from subgroups
)

select * from final
