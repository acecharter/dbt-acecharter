
select
    case
        when extract(month from CalendarDate) > 7
            then concat(
                extract(year from CalendarDate),
                '-',
                substr(cast((extract(year from CalendarDate) + 1) as string), 3, 2)
            )
        when extract(month from CalendarDate) <= 7
            then concat(
                extract(year from CalendarDate) - 1,
                '-',
                extract(year from CalendarDate) - 2000
            )
        else 'ERROR'
    end as SchoolYear,
    SchoolId,
    NameOfInstitution,
    date(CalendarDate) as CalendarDate,
    CalendarEvent,
    CountsTowardAttendance
from {{ source('StarterPack', 'CalendarDates') }}
