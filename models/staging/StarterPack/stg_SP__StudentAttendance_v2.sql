SELECT
  SchoolId,
  NameOfInstitution,
  StudentUniqueId,
  LastSurname,
  FirstName,
  ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
  CountOfAllAbsenceEvents AS CountOfDaysAbsent,
  CountOfAllInAttendanceEvents AS CountOfDaysInAttendance,
  CountOfDaysEnrolled
FROM {{ source('StarterPack', 'StudentAttendance_v2')}}