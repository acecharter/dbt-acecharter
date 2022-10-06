SELECT * EXCEPT (ExitWithdrawDate, ExitWithdrawReason)
FROM {{ ref('dim_Students')}}
WHERE IsCurrentlyEnrolled = TRUE