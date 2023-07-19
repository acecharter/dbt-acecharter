with current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

elpac_results as (
    select * except (AssessmentDate)
    from {{ ref('fct_StudentAssessment') }}
    where AceAssessmentId in ('8')
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
        er.* except (StateUniqueId, TestedSchoolId)
    from current_students as cs
    inner join elpac_results as er
        on cs.StateUniqueId = er.StateUniqueId
    left join schools as s
        on cs.SchoolId = s.SchoolId
)

select * from final
