WITH
  demographics AS (
    SELECT * FROM {{ ref('stg_SP__StudentDemographics')}}
    UNION ALL
    SELECT * FROM {{ ref('stg_SPA__StudentDemographics_SY22')}}
  ),

  final AS (
    SELECT
      SchoolYear,
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
    FROM demographics
  )

SELECT * FROM final