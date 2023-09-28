with calendar_dates as (
    select CalendarDate
    from unnest(
        generate_date_array(
            (select min(CalendarDate) from {{ ref('stg_SP__CalendarDates') }}),
            current_date('America/Los_Angeles'),
            interval 1 day)
    ) AS CalendarDate
),

schools as (
    select * from {{ ref('stg_SP__Schools') }}
),

school_dates as (
    select
        c.CalendarDate,
        s.SchoolYear,
        s.SchoolId
    from calendar_dates as c
    cross join schools as s
),

student_enrollments as (
    select * from {{ ref('stg_SP__StudentEnrollments') }}
),

joined as (
    select
        d.SchoolYear,
        d.SchoolId,
        d.CalendarDate,
        case
            when (
                e.SchoolYear = d.SchoolYear
                and e.EntryDate <= d.CalendarDate
                and e.ExitWithdrawDate >= d.CalendarDate
            ) then 1
            else 0
        end as UniqueEnrollment,
        case when e.EntryDate = d.CalendarDate then 1 else 0 end
            as EnrollmentEntry,
        case when e.ExitWithdrawDate = d.CalendarDate then 1 else 0 end
            as EnrollmentExit
    from school_dates as d
    full outer join student_enrollments as e
        on
            d.SchoolId = e.SchoolId
            and d.SchoolYear = e.SchoolYear
),

final as (
    select
        SchoolYear,
        SchoolId,
        CalendarDate,
        sum(UniqueEnrollment) as Enrollment,
        sum(EnrollmentEntry) as EnrollmentEntries,
        sum(EnrollmentExit) as EnrollmentExits
    from joined
    group by 1, 2, 3
)

select * from final
order by CalendarDate
