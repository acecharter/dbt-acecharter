with anet_m as (
    select *
    from {{ ref('stg_RD__Anet') }}
    where ScoredBy = 'Machine'
),

final as (
    select
        AceAssessmentId,
        AceAssessmentName,
        SchoolYear as Year,
        concat(
            cast(SchoolYear as string), '-', cast(SchoolYear - 1999 as string)
        ) as SchoolYear,
        StateSchoolCode,
        SchoolName,
        cast(SasId as string) as StateUniqueId,
        cast(SisId as string) as StudentUniqueId,
        StudentFirstName as FirstName,
        StudentMiddleName as MiddleName,
        StudentLastName as LastName,
        cast(EnrollmentGrade as int64) as GradeLevel,
        case
            when Course = 'english_i' then 'English I'
            when Course = 'english_ii' then 'English II'
            when Course = 'english_iii' then 'English III'
            when Course = 'algebra_i' then 'Algebra I'
            when Course = 'geometry' then 'Geometry'
            else Course
        end as Course,
        Period,
        TeacherFirstName,
        TeacherLastName,
        Cycle,
        Subject,
        AssessmentId,
        AssessmentName,
        sum(PointsReceived) as PointsReceived,
        sum(PointsPossible) as PointsPossible,
        round(sum(PointsReceived) / sum(PointsPossible), 2) as Score
    from anet_m
    group by
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
)

select *
from final
order by cycle
