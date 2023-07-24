--This script identifies "eligible" students for each testing window based on dates entered in the RenStarWindows table

with students as (
    select * from {{ ref('dim_Students') }}
),

test_windows as (
    select
        * except (EligibleStudentsEnrollmentDate),
        case
            when
                EligibleStudentsEnrollmentDate > current_date()
                then current_date()
            else EligibleStudentsEnrollmentDate
        end as EligibleStudentsEnrollmentDate
    from {{ ref('stg_GSD__RenStarTestingWindows') }}
    where TestingWindowStartDate < current_date()
    and SchoolYear != '2020-21'
),

final as (
    select
        test_windows.SchoolYear,
        test_windows.TestingWindow,
        students.SchoolId,
        students.StudentUniqueId
    from students
    cross join test_windows
    where
        students.EntryDate <= test_windows.EligibleStudentsEnrollmentDate
        and students.ExitWithdrawDate >= test_windows.EligibleStudentsEnrollmentDate
)

select * from final
order by 1, 2, 3, 4
