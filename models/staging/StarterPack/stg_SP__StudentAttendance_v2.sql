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
    where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts
),

school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

final as (
    select
        school_year.SchoolYear,
        source_table.*
    from source_table
    cross join school_year
)

select * from final
