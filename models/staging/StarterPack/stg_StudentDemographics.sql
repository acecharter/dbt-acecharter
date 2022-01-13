/*Fields dropped due to inaccuracy or lack of use:
    - HasIep,
    - Has504Plan,
    - Cohort.Identifier,
    - Cohort.Type
*/

SELECT
  StudentUniqueId,
  StateUniqueId,
  SisUniqueId,
  DisplayName,
  LastSurname AS LastName,
  FirstName,
  MiddleName,
  Birthdate AS BirthDate,
  BirthSex,
  Gender,
  RaceEthFedRollup AS RaceEthnicity,
  IsEll,
  EllStatus,
  HasFrl,
  FrlStatus,
  Email,
  IsCurrentlyEnrolled,
  CurrentSchoolId,
  CurrentNameOfInstitution,
  CAST(CurrentGradeLevel AS int64) AS CurrentGradeLevel
FROM {{ source('StarterPack', 'StudentDemographics')}}