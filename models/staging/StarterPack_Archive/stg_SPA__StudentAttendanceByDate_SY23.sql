select
    '2022-23' as SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    EventDate,
    case
        when AttendanceEventCategoryDescriptor = 'Unreconciled' then 'Absent'
    end as AttendanceEventCategoryDescriptor,
    EventDuration
from {{ source('StarterPack_Archive', 'StudentAttendanceByDate_SY23') }}
where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts
