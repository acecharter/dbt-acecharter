WITH students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
),

star_assessments AS (
    SELECT *
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE AceAssessmentId IN ('10', '11')
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
)


SELECT
  s.* EXCEPT (SchoolId),
  stu.* EXCEPT (
      ExitWithdrawReason
    ),
  a.* EXCEPT (StateUniqueId, TestedSchoolId)
FROM students AS stu
LEFT JOIN star_assessments AS a
ON stu.StateUniqueId = a.StateUniqueId
LEFT JOIN schools AS s
ON stu.SchoolId = s.SchoolId
WHERE StudentResult IS NOT NULL