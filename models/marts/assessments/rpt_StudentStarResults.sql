WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
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
  cs.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  a.* EXCEPT (StateUniqueId, TestedSchoolId)
FROM current_students AS cs
LEFT JOIN star_assessments AS a
ON cs.StateUniqueId = a.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
