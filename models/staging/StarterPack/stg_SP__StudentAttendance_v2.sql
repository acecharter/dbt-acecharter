SELECT
  SchoolId,
  NameOfInstitution,
  StudentUniqueId,
  LastSurname,
  FirstName,
  ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
  ROUND(1 - AverageDailyAttendance, 4) AS AbsenceRate,
  CountOfAllAbsenceEvents AS CountOfDaysAbsent,
  CountOfAllInAttendanceEvents AS CountOfDaysInAttendance,
  CountOfDaysEnrolled,
  CASE
      WHEN CountOfAllInAttendanceEvents = 0 THEN 'N/A (0 days attended)' 
      WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
      WHEN AverageDailyAttendance > 0.9 THEN 'No'
      WHEN AverageDailyAttendance <= 0.9 THEN 'Yes'
  END AS IsChronicallyAbsent,
  CASE
    WHEN CountOfAllInAttendanceEvents = 0 THEN 'N/A (0 days attended)' 
    WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
    WHEN AverageDailyAttendance >= 0.95 THEN 'Satisfactory'
    WHEN AverageDailyAttendance > 0.90 AND AverageDailyAttendance < 0.95 THEN 'At Risk'
    WHEN AverageDailyAttendance > 0.80 AND AverageDailyAttendance <= 0.90 THEN 'Moderate Chronic Absence'
    WHEN AverageDailyAttendance <= 0.80 THEN 'Severe Chronic Absence'
  END AS AttendanceRateGroup
FROM {{ source('StarterPack', 'StudentAttendance_v2')}}