WITH unioned AS (
  SELECT * FROM {{ ref('stg_SP__StudentEnrollments') }}
  UNION ALL
  SELECT * FROM {{ ref('stg_SPA__StudentEnrollments_SY22') }}
),

final AS (
  SELECT
    SchoolYear,
    SchoolId,
    StudentUniqueId,
    GradeLevel,
    EntryDate,
    ExitWithdrawDate,
    ExitWithdrawReason,
    IsCurrentEnrollment
  FROM unioned
)

SELECT * FROM final
