with schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

ada as (
    select * from {{ ref('fct_SchoolAverageDailyAttendance') }}
),

reporting_periods as (
    select
        a.EventDate,
        p.ReportingPeriod
    from ada as a
    cross join {{ ref('stg_RD__AttendanceReportingPeriods') }} as p
    where a.EventDate between p.StartDate and p.EndDate
    group by 1, 2
    order by a.EventDate
),

final as (
    select
        s.*,
        a.GradeLevel,
        p.ReportingPeriod,
        a.Month,
        a.MonthRank,
        a.WeekOf,
        a.EventDate,
        a.Apportionment,
        a.Possible,
        round(a.Apportionment / a.Possible, 4) as DailyAttendanceRate
    from ada as a
    left join schools as s
        on
            a.SchoolId = s.SchoolId
            and a.SchoolYear = s.SchoolYear
    left join reporting_periods as p
        on a.EventDate = p.EventDate
)

select * from final
