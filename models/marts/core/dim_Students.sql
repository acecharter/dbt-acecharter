WITH
  enrollments_ranked AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY SchoolYear, SchoolId, StudentUniqueId 
        ORDER BY SchoolYear, SchoolId, StudentUniqueId, EntryDate DESC
      ) AS Rank
    FROM {{ ref('fct_StudentSchoolEnrollments') }}
  ),

  demographics AS (
    SELECT * FROM {{ ref('dim_StudentDemographics')}}
  ),

  schools AS (
    SELECT DISTINCT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ ref('dim_Schools')}}
  ),
  
  final AS (
    SELECT
      e.SchoolYear,
      e.SchoolId,
      s.SchoolName,
      s.SchoolNameMid,
      s.SchoolNameShort,
      e.StudentUniqueId,
      d.StateUniqueId,
      d.DisplayName,
      d.LastName,
      d.FirstName,
      d.MiddleName,
      d.BirthDate,
      d.Gender,
      d.RaceEthnicity,
      d.IsEll,
      d.EllStatus,
      d.HasFrl,
      d.FrlStatus,
      d.HasIep,
      d.SeisEligibilityStatus,
      d.Email,
      e.GradeLevel,
      e.EntryDate,
      e.ExitWithdrawDate,
      e.ExitWithdrawReason,
      e.IsCurrentEnrollment AS IsCurrentlyEnrolled
    FROM enrollments_ranked AS e
    LEFT JOIN demographics AS d
    ON
      e.StudentUniqueId = d.StudentUniqueId
      AND e.SchoolYear = d.SchoolYear
    LEFT JOIN schools AS s
    ON e.SchoolId = s.SchoolId
    WHERE e.Rank = 1
  )

SELECT * FROM Final


