WITH
  sp_demographics AS (
    SELECT * FROM {{ ref('stg_SP__StudentDemographics')}}
  ),

  students_with_iep AS (
    SELECT * 
    FROM {{ ref('stg_RD__Seis')}}
    WHERE StudentEligibilityStatus = 'Eligible/Previously Eligible'
  ),

  demographics_final AS (
    SELECT
      sd.*,
      CASE
        WHEN i.StudentEligibilityStatus = 'Eligible/Previously Eligible' THEN TRUE
        ELSE FALSE
      END AS HasIep,
      i.StudentEligibilityStatus AS SeisEligibilityStatus
    FROM sp_demographics AS sd
    LEFT JOIN students_with_iep AS i
    USING (StateUniqueId)
  ),

  final AS (
    SELECT
      StudentUniqueId,
      StateUniqueId,
      SisUniqueId,
      DisplayName,
      LastSurname AS LastName,
      FirstName,
      MiddleName,
      Birthdate,
      BirthSex
      Gender,
      RaceEthnicity,
      CASE WHEN IsEll IS TRUE THEN 'Yes' ELSE 'No' END As IsEll,
      EllStatus,
      CASE WHEN HasFrl IS TRUE THEN 'Yes' ELSE 'No' END As HasFrl,
      FrlStatus,
      CASE WHEN HasIep IS TRUE THEN 'Yes' ELSE 'No' END As HasIep,
      SeisEligibilityStatus,
      Email,
      IsCurrentlyEnrolled
    FROM demographics_final
  )

SELECT * FROM final