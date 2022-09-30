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
  WHERE
    FirstName != 'Test' 
    AND LastSurname != 'Test'
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