SELECT
  '2021-22' AS SchoolYear,
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
  CASE WHEN HasIep IS TRUE THEN 'Eligible/Previously Eligible' ELSE NULL END AS SeisEligibilityStatus,
  Email,
  IsCurrentlyEnrolled,
  CurrentSchoolId,
  CurrentNameOfInstitution,
  CAST(CurrentGradeLevel AS int64) AS CurrentGradeLevel
FROM {{ source('StarterPack_Archive', 'StudentDemographics_SY22')}}
