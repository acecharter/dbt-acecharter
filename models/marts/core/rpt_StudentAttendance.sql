WITH schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
),

students AS (
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
  sc.*,
  st.* EXCEPT (SchoolId),
  a.* EXCEPT (SchoolId, StudentUniqueId)
FROM students AS st
LEFT JOIN attendance AS a
ON
  st.StudentUniqueId = a.StudentUniqueId AND
  st.SchoolId = a.SchoolId
LEFT JOIN schools AS sc
ON sc.SchoolId = a.SchoolId