WITH
  dates AS (
    SELECT *
    FROM {{ ref('stg_SP__CalendarDates') }}
    WHERE CalendarEvent = 'Instructional day'
    AND CalendarDate < CURRENT_DATE('America/Los_Angeles')
  ),

  student_enrollments AS (
    SELECT *
    FROM {{ ref('stg_SP__StudentEnrollments')}}
  ),

  joined AS (
    SELECT
      d.* EXCEPT(CalendarEvent),
      CASE
        WHEN (
            e.SchoolYear = d.SchoolYear
            AND e.EntryDate <= d.CalendarDate 
            AND e.ExitWithdrawDate >= d.CalendarDate
        ) THEN 1
        ELSE 0
      END UniqueEnrollment,
      CASE WHEN e.EntryDate = d.CalendarDate THEN 1 ELSE 0 END AS EnrollmentEntry,
      CASE WHEN e.ExitWithdrawDate = d.CalendarDate THEN 1 ELSE 0 END AS EnrollmentExit
    FROM dates as d
    LEFT JOIN student_enrollments AS e
    ON d.SchoolId = e.SchoolId
    AND d.SchoolYear = e.SchoolYear
  ),

  final AS (
    SELECT
      SchoolYear,
      SchoolId, 
      CalendarDate, 
      SUM(UniqueEnrollment) AS Enrollment,
      SUM(EnrollmentEntry) AS EnrollmentEntries,
      SUM(EnrollmentExit) AS EnrollmentExits
    FROM joined
    GROUP BY 1, 2, 3
  )

SELECT * FROM final