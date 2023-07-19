with current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

current_schools as (
    select
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_CurrentSchools') }}
),

assessment_results as (
    select *
    from {{ ref('fct_StudentAssessment') }}
),

assessment_info as (
    select
        AceAssessmentId,
        AssessmentSubject,
        AssessmentFamilyNameShort,
        SystemOrVendorName
    from {{ ref('stg_GSD__Assessments') }}
),

sbac_results as (
    select
        r.StateUniqueId,
        r.AssessmentSchoolYear,
        r.AceAssessmentId,
        r.AssessmentName,
        i.AssessmentSubject,
        i.SystemOrVendorName,
        r.AssessmentId,
        r.GradeLevelWhenAssessed,
        r.AssessmentGradeLevel,
        r.AssessmentObjective,
        cast(r.StudentResult as int64) as AchievementLevel,
        case
            when r.StudentResult = '1' then 'Not Met'
            when r.StudentResult = '2' then 'Nearly Met'
            when r.StudentResult = '3' then 'Met'
            when r.StudentResult = '4' then 'Exceeded'
        end as AchievementLevelCategory
    from assessment_results as r
    left join assessment_info as i
        on r.AceAssessmentId = i.AceAssessmentId
where
    i.SystemOrVendorName = 'CAASPP'
    and i.AssessmentFamilyNameShort = 'SBAC'
    and r.AssessmentObjective = 'Overall'
    and r.ReportingMethod = 'Achievement Level'
    and cast(r.StudentResult as int64) <= 4

)

select
s.* except (SchoolId),
cs.* except (
    ExitWithdrawDate,
    ExitWithdrawReason
),
r.* except (StateUniqueId)
from current_students as cs
left join sbac_results as r
on cs.StateUniqueId = r.StateUniqueId
left join current_schools as s
on cs.SchoolId = s.SchoolId
where AceAssessmentId is not null
