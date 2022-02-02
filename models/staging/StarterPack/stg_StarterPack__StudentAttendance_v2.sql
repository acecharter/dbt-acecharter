-- No fields dropped from source table

SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId,
  LastSurname,
  FirstName,
  ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
  ROUND(1 - AverageDailyAttendance, 4) AS AbsenceRate,
  CountOfAllAbsenceEvents AS CountOfDaysAbsent,
  CountOfAllInAttendanceEvents AS CountOfDaysInAttendance,
  CountOfDaysEnrolled,
  CASE
      WHEN AverageDailyAttendance > 0.9 THEN 'No'
      WHEN AverageDailyAttendance <= 0.9 THEN 'Yes'
  END AS IsChronicallyAbsent,
  CASE
    WHEN AverageDailyAttendance >= 0.95 THEN 'Satisfactory'
    WHEN AverageDailyAttendance > 0.90 AND AverageDailyAttendance < 0.95 THEN 'At Risk'
    WHEN AverageDailyAttendance > 0.80 AND AverageDailyAttendance <= 0.90 THEN 'Moderate Chronic Absence'
    WHEN AverageDailyAttendance <= 0.80 THEN 'Severe Chronic Absence'
  END AS AttendanceRateGroup
FROM {{ source('StarterPack', 'StudentAttendance_v2')}}