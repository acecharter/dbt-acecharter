SELECT * EXCEPT (ExitWithdrawDate, ExitWithdrawReason)
FROM {{ ref('dim_CurrentStudents') }}