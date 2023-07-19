select
    '2021-22' as SchoolYear,
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
from {{ source('StarterPack_Archive', 'StudentAttendanceByDate_SY22') }}
where StudentUniqueId != '16348' --This is a fake/test student in PS
