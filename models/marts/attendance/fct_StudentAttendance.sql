SELECT
  SchoolId,
  StudentUniqueId,
  CountOfDaysAbsent,
  CountOfDaysInAttendance,
  CountOfDaysEnrolled,
  ROUND(CountOfDaysAbsent/CountOfDaysEnrolled, 4) AS AbsenceRate,
  AverageDailyAttendance,
  CASE
    WHEN CountOfDaysInAttendance = 0 THEN 'N/A (0 days attended)' 
    WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
    WHEN AverageDailyAttendance >= 0.95 THEN 'Satisfactory'
    WHEN AverageDailyAttendance > 0.90 AND AverageDailyAttendance < 0.95 THEN 'At Risk'
    WHEN AverageDailyAttendance > 0.80 AND AverageDailyAttendance <= 0.90 THEN 'Moderate Chronic Absence'
    WHEN AverageDailyAttendance <= 0.80 THEN 'Severe Chronic Absence'
  END AS AttendanceRateGroup,
  CASE
    WHEN CountOfDaysInAttendance = 0 THEN 'N/A (0 days attended)' 
    WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
    WHEN AverageDailyAttendance > 0.9 THEN 'No'
    WHEN AverageDailyAttendance <= 0.9 THEN 'Yes'
  END AS IsChronicallyAbsent
FROM {{ ref('stg_SP__StudentAttendance_v2')}}