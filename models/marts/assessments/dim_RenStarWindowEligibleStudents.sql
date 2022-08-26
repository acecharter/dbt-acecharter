--This script identifies "eligible" students for each testing window
-- A student is considered eligible if they were enrolled anytime during testing window

WITH
  students AS (
    SELECT * FROM {{ ref('dim_Students')}}
  ),

  testing_windows AS (
    SELECT 
      * EXCEPT(EligibleStudentsEnrollmentDate),
      CASE
        WHEN EligibleStudentsEnrollmentDate > CURRENT_DATE() THEN CURRENT_DATE()
        ELSE EligibleStudentsEnrollmentDate
      END AS EligibleStudentsEnrollmentDate
    FROM {{ ref('stg_GSD__RenStarTestingWindows')}}
    WHERE TestingWindowStartDate < CURRENT_DATE()
  ),

  final AS (
    SELECT
        w.SchoolYear,
        w.TestingWindow,
        s.SchoolId,
        s.StudentUniqueId
    FROM students AS s
    CROSS JOIN testing_windows AS w
    WHERE
        s.EntryDate <= w.EligibleStudentsEnrollmentDate AND
        s.ExitWithdrawDate >= w.EligibleStudentsEnrollmentDate
  )

SELECT * FROM final
ORDER BY 1, 2, 3, 4

