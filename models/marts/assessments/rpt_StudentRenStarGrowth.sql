with
schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

students as (
    select *
    from {{ ref('dim_Students') }}
),

assessments as (
    select
        a.AceAssessmentId,
        a.AssessmentName,
        a.StateUniqueId,
        a.TestedSchoolId,
        schools.SchoolNameMid as TestedSchoolName,
        a.AssessmentSchoolYear,
        a.AssessmentId,
        a.AssessmentDate,
        a.GradeLevelWhenAssessed,
        a.AssessmentGradeLevel,
        a.AssessmentSubject,
        a.AssessmentObjective,
        a.ReportingMethod,
        cast(a.StudentResult as int) as StudentResult,
        case
            when cast(a.StudentResult as int) > 65 then 'High Growth'
            when
                cast(a.StudentResult as int) >= 50
                and cast(a.StudentResult as int) <= 65
                then 'Above Average Growth'
            when
                cast(a.StudentResult as int) >= 35
                and cast(a.StudentResult as int) < 50
                then 'Below Average Growth'
            when cast(a.StudentResult as int) < 35 then 'Low Growth'
        end as GrowthLevel,
        case
            when cast(a.StudentResult as int) >= 35 then 'Yes'
            else 'No'
        end as AtOrAboveAverageGrowth
    from {{ ref('fct_StudentAssessment') }} as a
    left join schools
        on
            a.TestedSchoolId = schools.SchoolId
            and a.AssessmentSchoolYear = schools.SchoolYear
    where
        a.ReportingMethod like 'SGP%'
        and a.ReportingMethod != 'SGP (current)'
),

final as (
    select
        schools.* except (SchoolYear, SchoolId),
        students.* except (SchoolYear),
        assessments.* except (StateUniqueId)
    from students
    left join assessments
        on
            students.StateUniqueId = assessments.StateUniqueId
            and students.SchoolId = assessments.TestedSchoolId
            and students.SchoolYear = assessments.AssessmentSchoolYear
    left join schools
        on
            students.SchoolId = schools.SchoolId
            and students.SchoolYear = schools.SchoolYear
    where assessments.AceAssessmentId is not null
)

select * from final
