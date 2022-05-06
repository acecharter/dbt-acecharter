WITH
  students_by_window AS (
    SELECT * FROM {{ ref('dim_RenStarWindowEligibleStudents')}}
  ),
  
  star_results AS (
    SELECT *
    FROM {{ref('int_RenStar__unioned')}}
  ),

  final AS (
    SELECT
      s.SchoolYear,
      s.TestingWindow,
      s.SchoolId,
      s.StudentUniqueId,
      s.AceAssessmentId,
      COUNT(r.AceAssessmentId) AS ResultCount
    FROM students_by_window AS s
    LEFT JOIN star_results AS r
    ON
      s.SchoolYear = r.SchoolYear
      AND s.TestingWindow = r.StarTestingWindow
      AND s.SchoolId = r.TestedSchoolId
      AND s.StudentUniqueId = r.StudentIdentifier
      AND s.AceAssessmentId = r.AceAssessmentId
    GROUP BY 1, 2, 3, 4, 5
  )

SELECT * FROM final
