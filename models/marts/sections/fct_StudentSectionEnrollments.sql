select
    SchoolId,
    SessionName,
    SectionIdentifier,
    ClassPeriodName,
    StudentUniqueId,
    BeginDate,
    EndDate,
    case
        when current_date('America/Los_Angeles') between BeginDate and EndDate
            then date_diff(current_date('America/Los_Angeles'), BeginDate, day)
        else date_diff(EndDate, BeginDate, day)
    end as CountDaysEnrolledInSection,
    coalesce (current_date('America/Los_Angeles') between BeginDate and EndDate,
    false) as IsCurrentSectionEnrollment
from {{ ref('stg_SP__CourseEnrollments_v2') }}
group by 1, 2, 3, 4, 5, 6, 7
order by 1, 2, 3, 4, 5, 6
