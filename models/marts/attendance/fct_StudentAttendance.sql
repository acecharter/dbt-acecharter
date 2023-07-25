with unioned as (
    select * from {{ ref('stg_SP__StudentAttendance_v2') }}
    union all
    select * from {{ ref('stg_SPA__StudentAttendance_v2_SY23') }}
    union all
    select * from {{ ref('stg_SPA__StudentAttendance_v2_SY22') }}
)

select distinct
    SchoolYear,
    SchoolId,
    StudentUniqueId,
    CountOfDaysAbsent,
    CountOfDaysInAttendance,
    CountOfDaysEnrolled,
    round(CountOfDaysAbsent / CountOfDaysEnrolled, 4) as AbsenceRate,
    AverageDailyAttendance,
    case
        when CountOfDaysInAttendance = 0 then 'N/A (0 days attended)'
        when CountOfDaysEnrolled < 31 then 'N/A (enrolled <31 days)'
        when AverageDailyAttendance >= 0.95 then 'Satisfactory'
        when
            AverageDailyAttendance > 0.90 and AverageDailyAttendance < 0.95
            then 'At Risk'
        when
            AverageDailyAttendance > 0.80 and AverageDailyAttendance <= 0.90
            then 'Moderate Chronic Absence'
        when AverageDailyAttendance <= 0.80 then 'Severe Chronic Absence'
    end as AttendanceRateGroup,
    case
        when CountOfDaysEnrolled < 31 then 'N/A (enrolled <31 days)'
        when AverageDailyAttendance > 0.9 then 'No'
        when AverageDailyAttendance <= 0.9 then 'Yes'
    end as IsChronicallyAbsent
from unioned
