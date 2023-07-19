select
    '2021-22' as SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    round(AverageDailyAttendance, 4) as AverageDailyAttendance,
    CountOfAllAbsenceEvents as CountOfDaysAbsent,
    CountOfAllInAttendanceEvents as CountOfDaysInAttendance,
    CountOfDaysEnrolled
from {{ source('StarterPack_Archive', 'StudentAttendance_v2_SY22') }}
where StudentUniqueId != '16348' --This is a fake/test student account
