with students as (
    select * from {{ ref('dim_Students') }}
),

schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

assessments as (
    select *
    from {{ ref('stg_GSD__Assessments') }}
),

student_completion as (
    select * from {{ ref('fct_StudentRenStarMultiWindowCompletion') }}
),

final as (
    select
        sc.* except (SchoolYear),
        st.* except (SchoolYear, SchoolId),
        c.AssessmentSubject as StarAssessmentSubject,
        a.AssessmentNameShort,
        a.AssessmentSubject,
        c.SchoolYear,
        c.TestingPeriod,
        c.PreTestStatus,
        c.PostTestStatus,
        c.CompletedPreAndPostTest,
        c.IncludeInPeriodCompletionRate
    from student_completion as c
    left join schools as sc
        on
            c.SchoolId = sc.SchoolId
            and c.SchoolYear = sc.SchoolYear
    left join students as st
        on
            c.StudentUniqueId = st.StudentUniqueId
            and c.SchoolId = st.SchoolId
            and c.SchoolYear = st.SchoolYear
    left join assessments as a
        on c.AssessmentSubject = a.AssessmentNameShort
)

select *
from final
order by
    SchoolId,
    DisplayName,
    StarAssessmentSubject,
    SchoolYear,
    TestingPeriod
