WITH students AS (
  SELECT * EXCEPT (LastName, FirstName, MiddleName, BirthDate, Email, ExitWithdrawReason)
  FROM {{ref('dim_Students')}}
),

attendance AS (
  SELECT
    SchoolId,
    StudentUniqueId,
    ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
    CASE
        WHEN AverageDailyAttendance >= 0.95 THEN 'Satisfactory'
        WHEN AverageDailyAttendance > 0.90 AND AverageDailyAttendance < 0.95 THEN 'At Risk'
        WHEN AverageDailyAttendance > 0.80 AND AverageDailyAttendance <= 0.90 THEN 'Moderate Chronic Absence'
        WHEN AverageDailyAttendance <= 0.80 THEN 'Severe Chronic Absence'
    END AS AttendanceRateGroup,
    CASE
        WHEN AverageDailyAttendance > 0.9 THEN 'No'
        WHEN AverageDailyAttendance <= 0.9 THEN 'Yes'
    END AS IsChronicallyAbsent,
    ROUND(1 - AverageDailyAttendance, 4) AS AbsenceRate
  
  FROM {{ ref('stg_StarterPack__StudentAttendance_v2') }}
)

SELECT
  s.*,
  a.* EXCEPT (SchoolId, StudentUniqueId)
FROM students AS s
LEFT JOIN attendance AS a
ON
  s.StudentUniqueId = a.StudentUniqueId AND
  s.SchoolId = a.SchoolId