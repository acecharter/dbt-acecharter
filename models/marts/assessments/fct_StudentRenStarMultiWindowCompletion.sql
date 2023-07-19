with completion_by_window as (
    select * from {{ ref('fct_StudentRenStarCompletionByWindow') }}
),

student_testing as (
    select distinct
        SchoolYear,
        SchoolId,
        StudentUniqueId,
        AssessmentSubject
    from completion_by_window
),

fall as (
    select *
    from completion_by_window
    where StarTestingWindow = 'Fall'
),

winter as (
    select *
    from completion_by_window
    where StarTestingWindow = 'Winter'
),

spring as (
    select *
    from completion_by_window
    where StarTestingWindow = 'Spring'
),

fall_spring as (
    select
        t.*,
        'Fall to Spring' as TestingPeriod,
        f.TestingStatus as PreTestStatus,
        s.TestingStatus as PostTestStatus,
        f.TestingRequiredBasedOnEnrollmentDates as PreTestRequired,
        s.TestingRequiredBasedOnEnrollmentDates as PostTestRequired
    from student_testing as t
    left join fall as f
        on
            t.SchoolYear = f.SchoolYear
            and t.SchoolId = f.SchoolId
            and t.StudentUniqueId = f.StudentUniqueId
            and t.AssessmentSubject = f.AssessmentSubject
    left join spring as s
        on
            t.SchoolYear = s.SchoolYear
            and t.SchoolId = s.SchoolId
            and t.StudentUniqueId = s.StudentUniqueId
            and t.AssessmentSubject = s.AssessmentSubject
),

fall_winter as (
    select
        t.*,
        'Fall to Winter' as TestingPeriod,
        f.TestingStatus as PreTestStatus,
        w.TestingStatus as PostTestStatus,
        f.TestingRequiredBasedOnEnrollmentDates as PreTestRequired,
        w.TestingRequiredBasedOnEnrollmentDates as PostTestRequired
    from student_testing as t
    left join fall as f
        on
            t.SchoolYear = f.SchoolYear
            and t.SchoolId = f.SchoolId
            and t.StudentUniqueId = f.StudentUniqueId
            and t.AssessmentSubject = f.AssessmentSubject
    left join winter as w
        on
            t.SchoolYear = w.SchoolYear
            and t.SchoolId = w.SchoolId
            and t.StudentUniqueId = w.StudentUniqueId
            and t.AssessmentSubject = w.AssessmentSubject
),

winter_spring as (
    select
        t.*,
        'Winter to Spring' as TestingPeriod,
        w.TestingStatus as PreTestStatus,
        s.TestingStatus as PostTestStatus,
        w.TestingRequiredBasedOnEnrollmentDates as PreTestRequired,
        s.TestingRequiredBasedOnEnrollmentDates as PostTestRequired
    from student_testing as t
    left join winter as w
        on
            t.SchoolYear = w.SchoolYear
            and t.SchoolId = w.SchoolId
            and t.StudentUniqueId = w.StudentUniqueId
            and t.AssessmentSubject = w.AssessmentSubject
    left join spring as s
        on
            t.SchoolYear = s.SchoolYear
            and t.SchoolId = s.SchoolId
            and t.StudentUniqueId = s.StudentUniqueId
            and t.AssessmentSubject = s.AssessmentSubject
),

unioned as (
    select * from fall_spring
    union all
    select * from fall_winter
    union all
    select * from winter_spring
),

final as (
    select
        *,
        case
            when
                PreTestRequired = 'Yes' and PostTestRequired = 'Yes'
                then 'Yes'
            else 'No'
        end as PreAndPostTestRequired,
        case
            when
                PreTestStatus = 'Tested' and PostTestStatus = 'Tested'
                then 'Yes'
            when
                PreTestStatus != 'Not Tested' and PostTestStatus != 'Not Tested'
                then 'Other'
            else 'No'
        end as CompletedPreAndPostTest,
        case
            when
                (PreTestRequired = 'Yes' or PreTestStatus = 'Tested')
                and (PostTestRequired = 'Yes' or PostTestStatus = 'Tested')
                then 'Yes'
            else 'No'
        end as IncludeInPeriodCompletionRate
    from unioned
)

select * from final where PostTestStatus is not null
