--This script identifies students who are "eligible" for Fall to Spring SGP results.
-- A student is considered eligible if they were enrolled by census day and remained enrolled thru the end of the year.

WITH students AS (
  SELECT * FROM {{ ref('dim_Students')}}
  WHERE
    EntryDate <= DATE('2021-10-06')
    AND IsCurrentlyEnrolled=TRUE
),

star_fall_to_winter_sgp_results AS (
  SELECT *
  FROM {{ ref('fct_StudentAssessment')}}
  WHERE
    AceAssessmentId IN ('10', '11')
    AND AssessmentSchoolYear = '2021-22'
    AND ReportingMethod = 'SGP (Fall to Spring)'
),

math AS (
  SELECT *
  FROM star_fall_to_winter_sgp_results
  WHERE AceAssessmentId = '10'
),

reading AS (
  SELECT *
  FROM star_fall_to_winter_sgp_results
  WHERE AceAssessmentId = '11'
),

students_math AS (
  SELECT DISTINCT
    students.*,
    'Math' AS AssessmentSubject,
    CASE WHEN star.StateUniqueId IS NOT NULL THEN 'Yes' ELSE 'No' END AS HasFallAndSpringResult 
  FROM students
  LEFT JOIN star_fall_to_winter_sgp_results AS star
  USING (StateUniqueId)
),

students_reading AS (
  SELECT DISTINCT
    students.*,
    'Reading' AS AssessmentSubject,
    CASE WHEN star.StateUniqueId IS NOT NULL THEN 'Yes' ELSE 'No' END AS HasFallAndSpringResult 
  FROM students
  LEFT JOIN star_fall_to_winter_sgp_results AS star
  USING (StateUniqueId)
),

final AS (
  SELECT * FROM students_math
  UNION ALL
  SELECT * FROM students_reading
)

SELECT SchoolId,AssessmentSubject,HasFallAndSpringResult,COUNT(*) FROM final
GROUP BY 1,2,3
ORDER BY 1,2,3
