WITH final_grades AS (
    SELECT *
    FROM {{ ref('stg_SP__CourseGrades') }}
    WHERE GradeTypeDescriptor = 'Final'
),

schools AS (
  SELECT * FROM {{ref('dim_Schools')}}
),

student_demographics AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
)

SELECT
  s.SchoolName,
  s.SchoolNameMid,
  s.SchoolNameShort,
  g.* Except(SchoolName, LastSurname, FirstName),
  d.* EXCEPT(SchoolId, StudentUniqueId)
FROM final_grades AS g
LEFT JOIN student_demographics AS d
ON
  g.StudentUniqueId = d.StudentUniqueId
  AND g.SchoolId = d.SchoolId
LEFT JOIN schools AS s
ON g.SchoolId = s.SchoolId