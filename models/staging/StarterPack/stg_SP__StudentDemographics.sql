/*Fields dropped due to inaccuracy or lack of use:
    - HasIep (replaced with SEIS data),
    - Has504Plan,
    - Cohort.Identifier,
    - Cohort.Type
*/

WITH student_demographics AS (
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

students_with_iep AS (
    SELECT * FROM {{ ref('stg_RD__Seis')}}
    WHERE StudentEligibilityStatus = 'Eligible/Previously Eligible'
)

SELECT
  sd.*,
  CASE
    WHEN i.StudentEligibilityStatus = 'Eligible/Previously Eligible' THEN TRUE
    ELSE FALSE
  END AS HasIep,
  i.StudentEligibilityStatus AS SeisEligibilityStatus
FROM student_demographics AS sd
LEFT JOIN students_with_iep AS i
USING (StateUniqueId)