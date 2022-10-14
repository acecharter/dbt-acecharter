WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students')}}
    WHERE IsCurrentlyEnrolled = TRUE
),

caaspp_results AS (
    SELECT
      * EXCEPT (AssessmentDate, StudentResult),
      CAST(StudentResult AS INT64) AS StudentResult
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE
      AceAssessmentId IN ('1', '2')
),

final AS (
  SELECT
    cs.* EXCEPT (SchoolYear, ExitWithdrawReason),
    cr.* EXCEPT (StateUniqueId, TestedSchoolId),
  FROM current_students AS cs
  INNER JOIN caaspp_results AS cr
  ON cs.StateUniqueId = cr.StateUniqueId
)

SELECT * FROM final
