WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
),

caaspp_results AS (
    SELECT
      * EXCEPT (AdministrationDate, StudentResult),
      CAST(StudentResult AS INT64) AS StudentResult
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE
      AceAssessmentId IN ('1', '2')
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
  cs.* EXCEPT (ExitWithdrawReason),
  cr.* EXCEPT (StateUniqueId, TestedSchoolId),
FROM current_students AS cs
INNER JOIN caaspp_results AS cr
ON cs.StateUniqueId = cr.StateUniqueId
LEFT JOIN schools AS s
ON cs.SchoolId = s.SchoolId
