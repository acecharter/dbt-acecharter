with hs_summer_enrollments as (
    select
        SchoolId,
        StudentUniqueId
    from {{ ref('stg_SPA__StudentEnrollments_SY23')}}
    where ExitWithdrawDate = '2023-07-08'
),

-- Remove 17 additional enrollment days for HS students whose summer enrollment were included in data (identified by a '2023-07-08' ExitWithdrawDate); attendance was not recorded for these days
final as (
    select
        '2022-23' as SchoolYear,
        a.SchoolId,
        a.NameOfInstitution,
        a.StudentUniqueId,
        a.LastSurname,
        a.FirstName,
        case
            when hs.StudentUniqueId is not null then round(a.CountOfAllInAttendanceEvents / (a.CountOfDaysEnrolled - 17), 4)
            else round(a.AverageDailyAttendance, 4)
        end as AverageDailyAttendance,
        a.CountOfAllAbsenceEvents as CountOfDaysAbsent,
        a.CountOfAllInAttendanceEvents as CountOfDaysInAttendance,
        case
            when hs.StudentUniqueId is not null then a.CountOfDaysEnrolled - 17
            else a.CountOfDaysEnrolled
        end as CountOfDaysEnrolled
    from {{ source('StarterPack_Archive', 'StudentAttendance_v2_SY23') }} as a
    left join hs_summer_enrollments as hs
    on a.StudentUniqueId = hs.StudentUniqueId
    where a.StudentUniqueId not in ('16671', '16667', '16668', '16860') -- These are fake/test student accounts
)

select * from final
