-- No fields dropped from source table

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