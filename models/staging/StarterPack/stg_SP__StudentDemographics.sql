/*Fields dropped due to inaccuracy or lack of use:
    - HasIep (replaced with SEIS data),
    - Has504Plan,
    - Cohort.Identifier,
    - Cohort.Type
*/

SELECT
  StudentUniqueId,
  StateUniqueId,
  SisUniqueId,
  DisplayName,
  LastSurname,
  FirstName,
  MiddleName,
  Birthdate,
  BirthSex,
  Gender,
  CASE
    WHEN RaceEthFedRollup = 'Hispanic or Latino of any race' THEN 'Hispanic or Latino' 
    WHEN RaceEthFedRollup IS NULL THEN 'Unknown/Missing'
    ELSE RaceEthFedRollup 
  END AS RaceEthnicity,
  IsEll,
  EllStatus,
  HasFrl,
  FrlStatus,
  Email,
  IsCurrentlyEnrolled,
  CurrentSchoolId,
  CurrentNameOfInstitution AS CurrentSchoolName,
  CAST(CurrentGradeLevel AS int64) AS CurrentGradeLevel
FROM {{ source('StarterPack', 'StudentDemographics')}}