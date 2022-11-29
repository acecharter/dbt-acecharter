WITH source_table AS (
  SELECT
    SchoolId,
    NameOfInstitution AS SchoolName,
    StudentUniqueId,
    LastSurname AS LastName,
    FirstName,
    CAST(GradeLevel AS int64) AS GradeLevel,
    EntryDate,
    ExitWithdrawDate,
    ExitWithdrawReason,
    IsCurrentEnrollment
  FROM {{ source('StarterPack', 'StudentEnrollments')}}
  WHERE StudentUniqueId NOT IN ('16671', '16667', '16668')  -- These are fake/test student accounts
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.*
  FROM source_table
  CROSS JOIN sy
)

SELECT * FROM final