WITH
  schools AS (
    SELECT 
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ref('dim_Schools')}}
  ),

  students AS (
    SELECT * EXCEPT(
      LastName,
      FirstName,
      MiddleName,
      BirthDate,
      Email
    )
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
      CASE
          WHEN CountOfDaysInAttendance = 0 THEN 'N/A (0 days attended)' 
          WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
          WHEN AverageDailyAttendance > 0.9 THEN 'No'
          WHEN AverageDailyAttendance <= 0.9 THEN 'Yes'
      END AS IsChronicallyAbsent,
      CASE
        WHEN CountOfDaysInAttendance = 0 THEN 'N/A (0 days attended)' 
        WHEN CountOfDaysEnrolled < 31 THEN 'N/A (enrolled <31 days)'
        WHEN AverageDailyAttendance >= 0.95 THEN 'Satisfactory'
        WHEN AverageDailyAttendance > 0.90 AND AverageDailyAttendance < 0.95 THEN 'At Risk'
        WHEN AverageDailyAttendance > 0.80 AND AverageDailyAttendance <= 0.90 THEN 'Moderate Chronic Absence'
        WHEN AverageDailyAttendance <= 0.80 THEN 'Severe Chronic Absence'
      END AS AttendanceRateGroup
    FROM {{ ref('stg_SP__StudentAttendance_v2')}}
  ),

  final AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      a.* EXCEPT (SchoolId, StudentUniqueId)
    FROM attendance AS a
    LEFT JOIN schools AS sc
    ON a.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      a.SchoolId = st.SchoolId
      AND a.StudentUniqueId = st.StudentUniqueId
  )

SELECT * FROM final