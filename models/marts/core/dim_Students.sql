WITH enrollments_ranked AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY SchoolId, StudentUniqueId 
            ORDER BY SchoolId, StudentUniqueId, EntryDate DESC
        ) Rank
    FROM {{ ref('stg_StarterPack__StudentEnrollments') }}
),
demographics AS (
    SELECT * FROM {{ ref('StudentDemographics_Seis__joined')}}
)


SELECT
    e.SchoolId,
    e.NameOfInstitution,
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
    d.IepStatusDate,
    d.Email,
    e.GradeLevel,
    d.IsCurrentlyEnrolled,
    e.EntryDate,
    e.ExitWithdrawDate,
    e.ExitWithdrawReason
FROM enrollments_ranked AS e
LEFT JOIN demographics AS d
USING (StudentUniqueId)
WHERE e.Rank = 1
