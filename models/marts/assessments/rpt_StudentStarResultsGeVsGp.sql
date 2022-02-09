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
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE AceAssessmentId IN ('10', '11')
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
        ReportingMethod,
        StudentResultDataType,
        StudentResult
      ),
  ge_vs_gp.* EXCEPT (AssessmentId)
FROM students AS stu
LEFT JOIN schools AS s
ON stu.SchoolId = s.SchoolId
LEFT JOIN ge_vs_gp
USING (AssessmentId)
LEFT JOIN star_assessments AS a
ON stu.StateUniqueId = a.StateUniqueId
WHERE a.StudentResult IS NOT NULL
AND stu.StateUniqueId = '7041884062'