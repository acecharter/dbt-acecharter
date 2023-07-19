with enrollments_ranked as (
    select
        *,
        rank() over (
            partition by SchoolYear, SchoolId, StudentUniqueId
            order by
                SchoolYear asc,
                SchoolId asc,
                StudentUniqueId asc,
                EntryDate desc
        ) as Rank
    from {{ ref('fct_StudentSchoolEnrollments') }}
),

demographics as (
    select * from {{ ref('dim_StudentDemographics') }}
),

final as (
    select
        e.SchoolYear,
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
        e.IsCurrentEnrollment as IsCurrentlyEnrolled
    from enrollments_ranked as e
    left join demographics as d
        on
            e.StudentUniqueId = d.StudentUniqueId
            and e.SchoolYear = d.SchoolYear
    where e.Rank = 1
)

select * from Final
