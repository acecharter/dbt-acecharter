with star as (
    select
        AceAssessmentId,
        AssessmentNameShort as AssessmentName,
        AssessmentSubject
    from {{ ref('stg_GSD__Assessments') }}
    where AssessmentFamilyNameShort = 'Star'
),

eligible as (
    select
        SchoolYear,
        SchoolId,
        StudentUniqueId,
        TestingWindow as StarTestingWindow
    from {{ ref('dim_RenStarWindowEligibleStudents') }}
),

star_eligible as (
    select
        eligible.*,
        star.*
    from eligible
    cross join star
),

result_counts as (
    select * from {{ ref('fct_StudentRenStarWindowResultCounts') }}
),

joined as (
    select
        coalesce (star_eligible.SchoolYear,
        result_counts.SchoolYear) as SchoolYear,
        coalesce (star_eligible.StarTestingWindow,
        result_counts.StarTestingWindow) as StarTestingWindow,
        coalesce (star_eligible.SchoolId, result_counts.SchoolId) as SchoolId,
        coalesce (star_eligible.StudentUniqueId,
        result_counts.StudentUniqueId) as StudentUniqueId,
        case
            when star_eligible.SchoolYear is not null then 'Yes' else 'No'
        end as TestingRequiredBasedOnEnrollmentDates,
        coalesce (star_eligible.AceAssessmentId,
        result_counts.AceAssessmentId) as AceAssessmentId,
        coalesce (star_eligible.AssessmentName,
        result_counts.AssessmentName) as AssessmentName,
        coalesce (result_counts.ResultCount, 0) as ResultCount,
        case
            when result_counts.ResultCount > 0 then 'Tested' else 'Not Tested'
        end as TestingStatus
    from star_eligible
    full outer join result_counts
        on
            star_eligible.SchoolYear = result_counts.SchoolYear
            and star_eligible.StarTestingWindow
            = result_counts.StarTestingWindow
            and star_eligible.SchoolId = result_counts.SchoolId
            and star_eligible.StudentUniqueId = result_counts.StudentUniqueId
            and star_eligible.AceAssessmentId = result_counts.AceAssessmentId
),

math as (
    select * from joined
    where AceAssessmentId = '10'
),

reading as (
    select * from joined
    where AceAssessmentId = '11'
),

math_spanish as (
    select * from joined
    where AceAssessmentId = '12'
),

reading_spanish as (
    select * from joined
    where AceAssessmentId = '13'
),

early_literacy as (
    select * from joined
    where AceAssessmentId = '21'
),

early_literacy_spanish as (
    select * from joined
    where AceAssessmentId = '22'
),

math_progress_monitoring as (
    select * from joined
    where AceAssessmentId = '23'
),

reading_progress_monitoring as (
    select * from joined
    where AceAssessmentId = '24'
),

math_joined as (
    select
        math.* except (AceAssessmentId, TestingStatus, ResultCount),
        math.AssessmentName as AssessmentSubject,
        coalesce(math.ResultCount, 0)
        + coalesce(pm.ResultCount, 0) as ResultCount,
        coalesce(s.ResultCount, 0) as ResultCountOther,
        case
            when
                (coalesce(math.ResultCount, 0) + coalesce(pm.ResultCount, 0))
                > 0
                then 'Tested'
            when coalesce(s.ResultCount, 0) > 0 then 'Other Tested (Spanish)'
            else 'Not Tested'
        end as TestingStatus
    from math
    left join math_progress_monitoring as pm
        on
            math.SchoolYear = pm.SchoolYear
            and math.StarTestingWindow = pm.StarTestingWindow
            and math.SchoolId = pm.SchoolId
            and math.StudentUniqueId = pm.StudentUniqueId
    left join math_spanish as s
        on
            math.SchoolYear = s.SchoolYear
            and math.StarTestingWindow = s.StarTestingWindow
            and math.SchoolId = s.SchoolId
            and math.StudentUniqueId = s.StudentUniqueId
),

reading_joined as (
    select
        reading.* except (AceAssessmentId, TestingStatus, ResultCount),
        reading.AssessmentName as AssessmentSubject,
        coalesce(reading.ResultCount, 0)
        + coalesce(pm.ResultCount, 0) as ResultCount,
        coalesce(el.ResultCount, 0)
        + coalesce(els.ResultCount, 0)
        + coalesce(s.ResultCount, 0) as ResultCountOther,
        case
            when
                (coalesce(reading.ResultCount, 0) + coalesce(pm.ResultCount, 0))
                > 0
                then 'Tested'
            when
                (
                    coalesce(el.ResultCount, 0)
                    + coalesce(els.ResultCount, 0)
                    + coalesce(s.ResultCount, 0)
                )
                > 0
                then 'Other Tested (Early Literacy or Spanish)'
            else 'Not Tested'
        end as TestingStatus
    from reading
    left join reading_progress_monitoring as pm
        on
            reading.SchoolYear = pm.SchoolYear
            and reading.StarTestingWindow = pm.StarTestingWindow
            and reading.SchoolId = pm.SchoolId
            and reading.StudentUniqueId = pm.StudentUniqueId
    left join early_literacy as el
        on
            reading.SchoolYear = el.SchoolYear
            and reading.StarTestingWindow = el.StarTestingWindow
            and reading.SchoolId = el.SchoolId
            and reading.StudentUniqueId = el.StudentUniqueId
    left join early_literacy_spanish as els
        on
            reading.SchoolYear = els.SchoolYear
            and reading.StarTestingWindow = els.StarTestingWindow
            and reading.SchoolId = els.SchoolId
            and reading.StudentUniqueId = els.StudentUniqueId
    left join reading_spanish as s
        on
            reading.SchoolYear = s.SchoolYear
            and reading.StarTestingWindow = s.StarTestingWindow
            and reading.SchoolId = s.SchoolId
            and reading.StudentUniqueId = s.StudentUniqueId
),

final as (
    select * from math_joined
    union all
    select * from reading_joined
)

select * from final
