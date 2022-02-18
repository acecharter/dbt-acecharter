WITH enrollments_ranked AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY SchoolId, StudentUniqueId 
            ORDER BY SchoolId, StudentUniqueId, EntryDate DESC
        ) Rank
    FROM {{ ref('stg_SP__StudentEnrollments') }}
),

demographics AS (
    SELECT * FROM {{ ref('stg_SP__StudentDemographics')}}
)


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
    CASE WHEN d.IsEll IS TRUE THEN 'Yes' ELSE 'No' END As IsEll,
    d.EllStatus,
    CASE WHEN d.HasFrl IS TRUE THEN 'Yes' ELSE 'No' END As HasFrl,
    d.FrlStatus,
    CASE WHEN d.HasIep IS TRUE THEN 'Yes' ELSE 'No' END As HasIep,
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
