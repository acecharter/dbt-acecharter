--This script identifies "eligible" students for each testing window
-- A student is considered eligible if they enrolled at least 2 weeks before the testing window end date and did not exit prior to the end date

WITH students AS (
  SELECT * FROM {{ ref('dim_Students')}}
),

testing_windows AS (
    SELECT *
    FROM {{ ref('stg_GSD__RenStarTestingWindows')}}
    WHERE TestingWindowStartDate < CURRENT_DATE()
),

student_window_combos AS (
    SELECT
        s.StudentUniqueId,
        s.SchoolId,
        w.TestingWindowType,
        w.TestingWindowName
    FROM students AS s
    CROSS JOIN testing_windows AS w
    WHERE
        s.EntryDate <= w.EnrollmentEligibilityCutoffDate AND
        s.ExitWithdrawDate >= w.TestingWindowEndDate
    ORDER BY SchoolId, StudentUniqueId, TestingWindowType, TestingWindowName
),

final AS(
  SELECT
    *,
    '10' AS AceAssessmentId
  FROM student_window_combos

  UNION ALL
  SELECT
    *,
    '11' AS AceAssessmentId
  FROM student_window_combos
)

SELECT * FROM final