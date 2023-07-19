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

completion_by_window as (
    select * from {{ ref('fct_StudentRenStarCompletionByWindow') }}
),

final as (
    select
        sc.* except (SchoolYear),
        st.* except (SchoolYear, SchoolId),
        c.AssessmentSubject as StarAssessmentSubject,
        a.AssessmentNameShort,
        a.AssessmentSubject,
        c.SchoolYear,
        c.StarTestingWindow as TestingWindow,
        c.* except (
            SchoolYear,
            StarTestingWindow,
            AssessmentSubject,
            SchoolId,
            StudentUniqueId,
            AssessmentName,
            TestingStatus
        ),
        c.TestingStatus as WindowTestingStatus
    from completion_by_window as c
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
    where st.StudentUniqueId is not null
)

select *
from final
order by
    SchoolNameMid,
    StarAssessmentSubject,
    SchoolYear,
    TestingWindow,
    GradeLevel,
    DisplayName
