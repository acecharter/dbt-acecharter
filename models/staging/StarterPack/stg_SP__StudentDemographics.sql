/*Fields dropped due to inaccuracy or lack of use:
    - HasIep (replaced with SEIS data),
    - Has504Plan,
    - Cohort.Identifier,
    - Cohort.Type
*/
WITH source_table AS (
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
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentStarterPackSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.*
  FROM source_table
  CROSS JOIN sy
)

SELECT * FROM final
