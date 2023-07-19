with calendar_dates as (
    select
        SchoolId,
        NameOfInstitution,
        date(CalendarDate) as CalendarDate,
        CalendarEvent,
        CountsTowardAttendance
    from {{ source('StarterPack', 'CalendarDates') }}
),

sy as (
    select
        concat(
            min(extract(year from CalendarDate)),
            '-',
            min(extract(year from CalendarDate) - 1999)
        ) as SchoolYear
    from calendar_dates
),

final as (
    select
        sy.SchoolYear,
        c.*
    from calendar_dates as c
    cross join sy
)

select * from final
