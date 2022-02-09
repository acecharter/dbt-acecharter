WITH schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
),

students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
),

star_assessments AS (
    SELECT *
    FROM {{ ref('dim_RenStarStudentAssessments')}}
),

ge_vs_gp AS (
    SELECT *
    FROM {{ ref('int_StudentStarResultsGeVsGp')}}
)

SELECT
  s.* EXCEPT (SchoolId),
  stu.* EXCEPT (
      ExitWithdrawReason
    ),
  a.* EXCEPT (
        StateUniqueId,
        TestedSchoolId,
        GradePlacement
      ),
  v.* EXCEPT (AssessmentId)
FROM students AS stu
LEFT JOIN schools AS s
ON stu.SchoolId = s.SchoolId
LEFT JOIN star_assessments AS a
ON stu.StateUniqueId = a.StateUniqueId
LEFT JOIN ge_vs_gp AS v
ON a.AssessmentId = v.AssessmentId