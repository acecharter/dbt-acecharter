with source_table as (
    select
        SchoolId,
        NameOfInstitution,
        StudentUniqueId,
        LastSurname,
        FirstName,
        round(AverageDailyAttendance, 4) as AverageDailyAttendance,
        CountOfAllAbsenceEvents as CountOfDaysAbsent,
        CountOfAllInAttendanceEvents as CountOfDaysInAttendance,
        CountOfDaysEnrolled
    from {{ source('StarterPack', 'StudentAttendance_v2') }}
    -- These are fake/test student accounts
    where StudentUniqueId not in ('16671', '16667', '16668')
),

sy as (
    select * from {{ ref('dim_CurrentSchoolYear') }}
),

final as (
    select
        sy.SchoolYear,
        source_table.*
    from source_table
    cross join sy
)

select * from final
