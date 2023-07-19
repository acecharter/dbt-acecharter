with current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

all_students as (
    select
        SchoolYear,
        SchoolId,
        'All Students' as StudentGroupType,
        'All Students' as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 5
),

race_ethnicity as (
    select
        SchoolYear,
        SchoolId,
        'Race/Ethnicity' as StudentGroupType,
        RaceEthnicity as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 4, 5
    order by 1, 2, 4, 5
),

el_status as (
    select
        SchoolYear,
        SchoolId,
        'English Learner Status' as StudentGroupType,
        case
            when IsEll = 'Yes' then 'English Learner'
            when IsEll = 'No' then 'Not English Learner'
        end as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 4, 5
    order by 1, 2, 4, 5
),

frl_status as (
    select
        SchoolYear,
        SchoolId,
        'Free/Reduced Meal Eligibility Status' as StudentGroupType,
        case
            when HasFrl = 'Yes' then 'Free/Reduced Meal-Eligible'
            when HasFrl = 'No' then 'Not Free/Reduced Meal-Eligible'
        end as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 4, 5
    order by 1, 2, 4, 5
),

sped_status as (
    select
        SchoolYear,
        SchoolId,
        'Special Education Status' as StudentGroupType,
        case
            when HasIep = 'Yes' then 'Special Education'
            when HasIep = 'No' then 'Not Special Education'
        end as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 4, 5
    order by 1, 2, 4, 5
),

gender as (
    select
        SchoolYear,
        SchoolId,
        'Gender' as StudentGroupType,
        Gender as StudentGroup,
        GradeLevel,
        count(*) as Enrollment
    from current_students
    group by 1, 2, 4, 5
    order by 1, 2, 4, 5
),

final as (
    select * from all_students
    union all
    select * from race_ethnicity
    union all
    select * from el_status
    union all
    select * from frl_status
    union all
    select * from sped_status
    union all
    select * from gender
)

select * from final
