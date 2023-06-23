with 
  schools AS (
    select
      SchoolYear,
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    from {{ref('dim_Schools')}}
  ),

  enrollment AS (
    select * from {{ref('fct_SchoolEnrollmentByDate')}}
  ),

  reporting_periods AS (
    select
      enrollment.CalendarDate,
      reporting_periods.ReportingPeriod
    from enrollment
    cross join {{ ref('stg_RD__AttendanceReportingPeriods')}} as reporting_periods
    where enrollment.CalendarDate between reporting_periods.StartDate and reporting_periods.EndDate
    group by 1, 2
    order by enrollment.CalendarDate
  ),

  final AS (
    select
      schools.*,
      reporting_periods.ReportingPeriod,
      e.* except(SchoolId, SchoolYear)
    from schools
    right join enrollment as e
    on schools.SchoolId = e.SchoolId
    and schools.SchoolYear = e.SchoolYear
    left join reporting_periods
    on e.CalendarDate = reporting_periods.CalendarDate
    order by schools.SchoolName, e.CalendarDate --used alias for enrollment; without alias ordering by CalendarDate results in an error
  )

select * from final
