with schools as (
    select distinct
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

assessments as (
    select
        a.AceAssessmentId,
        a.AssessmentName,
        a.StateUniqueId,
        a.TestedSchoolId,
        case
            when a.TestedSchoolId = s.SchoolId then s.SchoolNameShort
            else 'Non-ACE School'
        end as TestedSchoolName,
        a.AssessmentSchoolYear,
        a.AssessmentId,
        a.AssessmentDate,
        a.GradeLevelWhenAssessed,
        a.AssessmentGradeLevel,
        a.AssessmentSubject,
        a.AssessmentObjective,
        a.ReportingMethod,
        a.StudentResultDataType,
        a.StudentResult
    from {{ ref('fct_StudentAssessment') }} as a
    left join schools as s
        on a.TestedSchoolId = s.SchoolId
),

final as (
    select
        s.* except (SchoolId),
        st.* except (SchoolYear),
        a.* except (StateUniqueId)
    from current_students as st
    inner join assessments as a
        on st.StateUniqueId = a.StateUniqueId
    inner join schools as s
        on st.SchoolId = s.SchoolId
)

select * from final
