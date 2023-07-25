with calendar_dates as (
    select *
    from {{ ref('stg_SP__CalendarDates') }}
    where CountsTowardAttendance = true
    union all
    select *
    from {{ ref('stg_SPA__CalendarDates_SY23') }}
    where CountsTowardAttendance = true
    union all
    select *
    from {{ ref('stg_SPA__CalendarDates_SY22') }}
    where CountsTowardAttendance = true
),

final as (
    select
        SchoolYear,
        min(CalendarDate) as StartDate,
        max(CalendarDate) as EndDate,
        row_number() over (order by SchoolYear desc) - 1 as YearsPriorToCurrent
    from calendar_dates
    group by SchoolYear
    order by SchoolYear desc
)

select distinct * from final
