select * except (ExitWithdrawDate, ExitWithdrawReason)
from {{ ref('dim_Students') }}
where IsCurrentlyEnrolled = true
