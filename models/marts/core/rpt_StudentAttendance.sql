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
    AverageDailyAttendance,
    AbsenceRate,
    CountOfDaysAbsent,
    CountOfDaysInAttendance,
    CountOfDaysEnrolled,
    IsChronicallyAbsent,
    AttendanceRateGroup  
  FROM {{ ref('stg_SP__StudentAttendance_v2') }}
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