WITH
  enrollments_ranked AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY SchoolId, StudentUniqueId 
        ORDER BY SchoolId, StudentUniqueId, EntryDate DESC
      ) Rank
    FROM {{ ref('dim_StudentEnrollments') }}
  ),

  demographics AS (
    SELECT * FROM {{ ref('dim_StudentDemographics')}}
  ),
  
  final AS (
    SELECT
      e.SchoolId,
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
    USING (StudentUniqueId)
    WHERE e.Rank = 1
  )

SELECT * FROM Final


