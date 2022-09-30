/*Fields dropped due to inaccuracy or lack of use:
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
    Has504Plan,
    HasIep,
    Email,
    IsCurrentlyEnrolled,
    CurrentSchoolId,
    CurrentNameOfInstitution AS CurrentSchoolName,
    CAST(CurrentGradeLevel AS int64) AS CurrentGradeLevel
  FROM {{ source('StarterPack', 'StudentDemographics')}}
),

students_with_iep AS (
  SELECT * 
  FROM {{ ref('stg_RD__Seis')}}
  WHERE StudentEligibilityStatus = 'Eligible/Previously Eligible'
  AND SpedExitDate IS NULL
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.* EXCEPT(
      HasIep,
      Email,
      IsCurrentlyEnrolled,
      CurrentSchoolId,
      CurrentSchoolName,
      CurrentGradeLevel),
    CASE
      WHEN i.StudentEligibilityStatus = 'Eligible/Previously Eligible' THEN TRUE
      ELSE FALSE
    END AS HasIep,
    i.StudentEligibilityStatus AS SeisEligibilityStatus,
    source_table.Email,
    source_table.IsCurrentlyEnrolled,
    source_table.CurrentSchoolId,
    source_table.CurrentSchoolName,
    source_table.CurrentGradeLevel
  FROM source_table
  CROSS JOIN sy
  LEFT JOIN students_with_iep AS i
  ON source_table.StateUniqueId = i.StateUniqueId
)

SELECT * FROM final
