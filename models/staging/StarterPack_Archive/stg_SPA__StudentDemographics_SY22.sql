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
  Has504Plan,
  HasIep,
  Email,
  IsCurrentlyEnrolled,
  CurrentSchoolId,
  CurrentNameOfInstitution AS CurrentSchoolName,
  CAST(CurrentGradeLevel AS int64) AS CurrentGradeLevel
FROM {{ source('StarterPack_Archive', 'StudentDemographics_SY22')}}
