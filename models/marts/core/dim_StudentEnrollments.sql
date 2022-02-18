SELECT
  SchoolId,
  StudentUniqueId,
  GradeLevel,
  EntryDate,
  ExitWithdrawDate,
  ExitWithdrawReason,
  IsCurrentEnrollment
FROM {{ ref('stg_SP__StudentEnrollments') }}