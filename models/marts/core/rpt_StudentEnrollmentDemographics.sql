WITH current_students AS (
    SELECT *
    FROM {{ ref('dim_Students') }}
    WHERE IsCurrentlyEnrolled = true
)


SELECT
  * EXCEPT (
      ExitWithdrawDate,
      ExitWithdrawReason
    )
FROM current_students