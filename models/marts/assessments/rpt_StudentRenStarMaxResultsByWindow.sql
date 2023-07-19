with schools as (
    select distinct
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

students as (
    select * from {{ ref('dim_Students') }}
),

star_results as (
    select
        r.AceAssessmentId,
        r.AssessmentName,
        r.StateUniqueId,
        r.TestedSchoolId,
        case
            when r.TestedSchoolId = s.SchoolId then s.SchoolNameShort
            else 'Non-ACE School'
        end as TestedSchoolName,
        r.SchoolYear as AssessmentSchoolYear,
        r.StarTestingWindow,
        r.AssessmentGradeLevel,
        r.AssessmentSubject,
        r.ReportingMethod,
        max(cast(r.StudentResult as float64)) as MaxStudentResult
    from {{ ref('int_RenStar_unpivoted') }} as r
    left join schools as s
        on r.TestedSchoolId = s.SchoolId
    where StudentResultDataType in ('INT64', 'FLOAT64')
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

final as (
    select
        schools.* except (SchoolId),
        students.* except (SchoolYear),
        star_results.* except (StateUniqueId)
    from students
    inner join star_results
        on
            students.StateUniqueId = star_results.StateUniqueId
            and students.SchoolYear = star_results.AssessmentSchoolYear
    inner join schools
        on students.SchoolId = schools.SchoolId
)

select * from final
