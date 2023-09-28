with schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

enrollment as (
    select * from {{ ref('fct_SchoolEnrollmentByDate') }}
),

-- Adjust reporting period EndDate to include subsequent dates (weekends/holidays) prior to the start of the next reporting periods to avoid losing students exited on these days
reporting_period_incl_wknd as (
    select
        * except(EndDate),
        coalesce(
            lead(StartDate) over (partition by SchoolYear order by StartDate asc) - 1,
            EndDate
        ) as EndDate
    from {{ ref('stg_RD__AttendanceReportingPeriods') }}
),

reporting_periods as (
    select
        enrollment.CalendarDate,
        reporting_period_incl_wknd.ReportingPeriod
    from enrollment
    cross join reporting_period_incl_wknd
    where
        enrollment.CalendarDate between reporting_period_incl_wknd.StartDate and reporting_period_incl_wknd.EndDate
    group by 1, 2
    order by enrollment.CalendarDate
),

final as (
    select
        schools.*,
        reporting_periods.ReportingPeriod,
        e.* except (SchoolId, SchoolYear)
    from schools
    right join enrollment as e
        on
            schools.SchoolId = e.SchoolId
            and schools.SchoolYear = e.SchoolYear
    left join reporting_periods
        on e.CalendarDate = reporting_periods.CalendarDate
    order by schools.SchoolName, e.CalendarDate --used alias for enrollment; without alias ordering by CalendarDate results in an error
)

select * from final
order by CalendarDate