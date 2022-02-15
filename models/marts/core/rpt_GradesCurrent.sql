WITH current_grades AS (
  SELECT *
  FROM {{ ref('stg_StarterPack__CourseGrades') }}
  WHERE
    IsCurrentGradingPeriod = true
    AND IsCurrentCourseEnrollment = true
),

student_demographics AS (
  SELECT *
  FROM {{ ref('dim_Students') }}
)

SELECT
 g.* Except(LastSurname, FirstName),
 d.* EXCEPT(SchoolId, StudentUniqueId)
FROM current_grades AS g
LEFT JOIN student_demographics AS d
ON
  g.StudentUniqueId = d.StudentUniqueId
  AND g.SchoolId = d.SchoolId