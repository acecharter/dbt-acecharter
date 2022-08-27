WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_CurrentStudents') }}
),

elpac_results AS (
    SELECT
      * EXCEPT (AssessmentDate)
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE
      AceAssessmentId IN ('8')
),

schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_CurrentSchools')}}
),

final AS (
  SELECT
    s.*,
    cs.* EXCEPT (SchoolId, SchoolYear, ExitWithdrawReason),
    er.* EXCEPT (StateUniqueId, TestedSchoolId),
  FROM current_students AS cs
  INNER JOIN elpac_results AS er
  ON cs.StateUniqueId = er.StateUniqueId
  LEFT JOIN schools AS s
  ON cs.SchoolId = s.SchoolId
)

SELECT * FROM final
