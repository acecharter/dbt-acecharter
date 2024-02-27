with current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

caaspp_results as (
    select
        * except (AssessmentDate, StudentResult),
        cast(StudentResult as int64) as StudentResult
    from {{ ref('fct_StudentAssessment') }}
    where
        AceAssessmentId in ('1', '2', '6')
),

schools as (
    select
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_CurrentSchools') }}
),

final as (
    select
        s.*,
        cs.* except (SchoolId, SchoolYear, ExitWithdrawReason),
        cr.* except (StateUniqueId, TestedSchoolId)
    from current_students as cs
    inner join caaspp_results as cr
        on cs.StateUniqueId = cr.StateUniqueId
    left join schools as s
        on cs.SchoolId = s.SchoolId
)

select * from final
