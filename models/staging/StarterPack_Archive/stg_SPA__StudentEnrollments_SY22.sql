SELECT
  '2021-22' AS SchoolYear,
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
FROM {{ source('StarterPack_Archive', 'StudentEnrollments_SY22')}}