WITH absences AS (
  SELECT * FROM {{ ref('stg_SP__StudentAttendanceByDate')}}
  WHERE AttendanceEventCategoryDescriptor= 'Absent'
),

school_days AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY SchoolId
      ORDER BY CalendarDate DESC
    ) AS Rank
  FROM  {{ ref('stg_SP__CalendarDates')}}
  WHERE CountsTowardAttendance IS TRUE
  AND CalendarDate < CURRENT_DATE()
  ORDER BY CalendarDate DESC
),

most_recent_10_days AS (
  SELECT * EXCEPT(Rank) 
  FROM school_days
  WHERE Rank <=10
),

students AS (
  SELECT *
  FROM {{ ref('dim_Students')}}
  WHERE IsCurrentlyEnrolled = TRUE
),

student_enrollments_by_date AS (
  SELECT
    s.SchoolId,
    s.StudentUniqueId,
    s.StateUniqueId,
    s.DisplayName,
    s.EntryDate,
    s.ExitWithdrawDate,
    e.CalendarDate
  FROM students AS s
  CROSS JOIN most_recent_10_days AS e
  WHERE
  e.CalendarDate >= s.EntryDate
  AND e.CalendarDate <= s.ExitWithdrawDate
  AND s.SchoolId = e.SchoolId
),

student_attendance_by_date AS (
  SELECT
    e.*,
    CASE WHEN a.EventDate IS NOT NULL THEN 'Absent' ELSE 'Present' END AS AttendanceStatus
  FROM student_enrollments_by_date AS e
  LEFT JOIN absences AS a
  ON e.SchoolId = a.SchoolId
  AND e.StudentUniqueId = a.StudentUniqueId
  AND e.CalendarDate = a.EventDate
),

student_attendance_aggregated AS (
  SELECT
    * EXCEPT (EntryDate, ExitWithdrawDate, CalendarDate),
    COUNT(*) AS StatusCount
  FROM student_attendance_by_date
  GROUP BY 1, 2, 3, 4, 5
),

stu_att_absent AS (
  SELECT * FROM student_attendance_aggregated
  WHERE AttendanceStatus = 'Absent'
),

stu_att_present AS (
  SELECT * FROM student_attendance_aggregated
  WHERE AttendanceStatus = 'Present'
),

stu_att_final AS (
  SELECT
    s.SchoolId,
    s.StudentUniqueId,
    s.StateUniqueId,
    s.DisplayName,
    s.GradeLevel,
    IFNULL(a.StatusCount,0) AS AbsenceCount,
    IFNULL(a.StatusCount,0) + IFNULL(p.StatusCount,0) AS DaysEnrolledCount,
    CASE
      WHEN a.StatusCount IS NULL AND p.StatusCount IS NULL THEN NULL
      ELSE ROUND(IFNULL(p.StatusCount,0) / (IFNULL(a.StatusCount,0) + IFNULL(p.StatusCount,0)),4)
    END AS AttendanceRate
  FROM students AS s
  LEFT JOIN stu_att_present AS p
  ON s.SchoolId = p.SchoolId
  AND s.StudentUniqueId = p.StudentUniqueId 
  LEFT JOIN stu_att_absent AS a
  ON s.SchoolId = a.SchoolId
  AND s.StudentUniqueId = a.StudentUniqueId 

)

SELECT * from stu_att_final
ORDER BY SchoolId, DisplayName