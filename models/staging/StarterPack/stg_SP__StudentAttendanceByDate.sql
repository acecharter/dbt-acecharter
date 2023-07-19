select
    case
        when extract(month from EventDate) > 7
            then concat(
                extract(year from EventDate),
                '-',
                substr(cast((extract(year from EventDate) + 1) as string), 3, 2)
            )
        when extract(month from EventDate) <= 7
            then concat(
                extract(year from EventDate) - 1,
                '-',
                extract(year from EventDate) - 2000
            )
        else 'ERROR'
    end as SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    EventDate,
    case
        when
            AttendanceEventCategoryDescriptor = 'Unreconciled'
            then 'Absent'
        else AttendanceEventCategoryDescriptor
    end as AttendanceEventCategoryDescriptor,
    EventDuration
from {{ source('StarterPack', 'StudentAttendanceByDate') }}
where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts