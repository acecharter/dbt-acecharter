WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
),

assessments AS (
    SELECT *
    FROM {{ ref('fct_StudentAssessment')}}
)


SELECT
  s.* EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    ),
  a.* EXCEPT (StateUniqueId, SchoolId)
FROM current_students AS s
LEFT JOIN assessments AS a
USING(StateUniqueId)