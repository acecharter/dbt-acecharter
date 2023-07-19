with dates as (
    select *
    from {{ ref('stg_SP__CalendarDates') }}
    where
        CalendarEvent = 'Instructional day'
        and CalendarDate < current_date('America/Los_Angeles')
),

student_enrollments as (
    select *
    from {{ ref('stg_SP__StudentEnrollments') }}
),

joined as (
    select
        d.* except (CalendarEvent),
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
    from dates as d
    left join student_enrollments as e
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
