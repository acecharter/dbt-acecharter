with absences as (
    select *
    from {{ ref('stg_SP__StudentAttendanceByDate') }}
    where AttendanceEventCategoryDescriptor = 'Absent'
),

school_days as (
    select
        *,
        rank() over (
            partition by SchoolId
            order by CalendarDate desc
        ) as Rank
    from {{ ref('stg_SP__CalendarDates') }}
    where
        CountsTowardAttendance is true
        and CalendarDate < current_date()
    order by CalendarDate desc
),

most_recent_10_days as (
    select * except (Rank)
    from school_days
    where Rank <= 10
),

students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

student_enrollments_by_date as (
    select
        s.SchoolId,
        s.StudentUniqueId,
        s.StateUniqueId,
        s.DisplayName,
        s.EntryDate,
        s.ExitWithdrawDate,
        e.CalendarDate
    from students as s
    cross join most_recent_10_days as e
    where
        e.CalendarDate >= s.EntryDate
        and e.CalendarDate <= s.ExitWithdrawDate
        and s.SchoolId = e.SchoolId
),

student_attendance_by_date as (
    select
        e.*,
        case when a.EventDate is not null then 'Absent' else 'Present' end
            as AttendanceStatus
    from student_enrollments_by_date as e
    left join absences as a
        on
            e.SchoolId = a.SchoolId
            and e.StudentUniqueId = a.StudentUniqueId
            and e.CalendarDate = a.EventDate
),

student_attendance_aggregated as (
    select
        * except (EntryDate, ExitWithdrawDate, CalendarDate),
        count(*) as StatusCount
    from student_attendance_by_date
    group by 1, 2, 3, 4, 5
),

stu_att_absent as (
    select * from student_attendance_aggregated
    where AttendanceStatus = 'Absent'
),

stu_att_present as (
    select * from student_attendance_aggregated
    where AttendanceStatus = 'Present'
),

stu_att_final as (
    select
        s.SchoolId,
        s.StudentUniqueId,
        s.StateUniqueId,
        s.DisplayName,
        s.GradeLevel,
        coalesce(a.StatusCount, 0) as AbsenceCount,
        coalesce(a.StatusCount, 0)
        + coalesce(p.StatusCount, 0) as DaysEnrolledCount,
        case
            when a.StatusCount is null and p.StatusCount is null then null
            else
                round(
                    coalesce(p.StatusCount, 0)
                    / (coalesce(a.StatusCount, 0) + coalesce(p.StatusCount, 0)),
                    4
                )
        end as AttendanceRate
    from students as s
    left join stu_att_present as p
        on
            s.SchoolId = p.SchoolId
            and s.StudentUniqueId = p.StudentUniqueId
    left join stu_att_absent as a
        on
            s.SchoolId = a.SchoolId
            and s.StudentUniqueId = a.StudentUniqueId

)

select * from stu_att_final
order by SchoolId, DisplayName
