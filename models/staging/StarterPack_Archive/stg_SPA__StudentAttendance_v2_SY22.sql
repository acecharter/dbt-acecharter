SELECT
    '2021-22' AS SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
    CountOfAllAbsenceEvents AS CountOfDaysAbsent,
    CountOfAllInAttendanceEvents AS CountOfDaysInAttendance,
    CountOfDaysEnrolled
FROM {{ source('StarterPack_Archive', 'StudentAttendance_v2_SY22')}}
WHERE StudentUniqueId != '16348' --This is a fake/test student account