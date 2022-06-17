WITH
  dates AS (
    SELECT *
    FROM {{ ref('stg_SP__CalendarDates') }}
    WHERE CalendarEvent = 'Instructional day'
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
            e.EntryDate <= d.CalendarDate 
            AND e.ExitWithdrawDate >= d.CalendarDate
        ) THEN 1
        ELSE 0
      END UniqueEnrollment,
      CASE WHEN e.EntryDate = d.CalendarDate THEN 1 ELSE 0 END AS EnrollmentEntry,
      CASE WHEN e.ExitWithdrawDate = d.CalendarDate THEN 1 ELSE 0 END AS EnrollmentExit
    FROM dates as d
    LEFT JOIN student_enrollments AS e
    USING (SchoolId)
  ),

  final AS (
    SELECT
      SchoolId, 
      CalendarDate, 
      SUM(UniqueEnrollment) AS Enrollment,
      SUM(EnrollmentEntry) AS EnrollmentEntries,
      SUM(EnrollmentExit) AS EnrollmentExits
    FROM joined
    GROUP BY 1, 2
  )

SELECT * FROM final