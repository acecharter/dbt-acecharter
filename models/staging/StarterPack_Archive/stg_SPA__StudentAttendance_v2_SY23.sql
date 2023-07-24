select
    '2022-23' as SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    round(AverageDailyAttendance, 4) as AverageDailyAttendance,
    CountOfAllAbsenceEvents as CountOfDaysAbsent,
    CountOfAllInAttendanceEvents as CountOfDaysInAttendance,
    CountOfDaysEnrolled
from {{ source('StarterPack_Archive', 'StudentAttendance_v2_SY23') }}
where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts
