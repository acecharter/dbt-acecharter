-- No fields dropped from source table

SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId,
  LastSurname,
  FirstName,
  AverageDailyAttendance,
  SumOfAllAbsenceEventDurations,
  CountOfAllAbsenceEvents,
  SumOfAllInAttendanceEventDurations,
  CountOfAllInAttendanceEvents,
  CountOfDaysEnrolled,
  SumOfUnreconciledAbsenceEventDurations,
  CountOfUnreconciledAbsenceEvents
FROM {{ source('StarterPack', 'StudentAttendance_v2')}}