WITH
  calendar_dates as (
    select * from {{ref('stg_SP__CalendarDates')}} where CountsTowardAttendance = true
    union all
    select * from {{ref('stg_SPA__CalendarDates_SY22')}} where CountsTowardAttendance = true
  ),

  final as (
    select
      SchoolYear,
      MIN(CalendarDate) as StartDate,
      MAX(CalendarDate) as EndDate,
      ROW_NUMBER() OVER (ORDER BY SchoolYear DESC) - 1 AS YearsPriorToCurrent
    from calendar_dates
    group by SchoolYear
    order by SchoolYear DESC
  )
  
select * from final