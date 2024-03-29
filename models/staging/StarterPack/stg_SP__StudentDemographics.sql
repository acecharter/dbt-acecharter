/*Fields dropped due to inaccuracy or lack of use:
    - Cohort.Identifier,
    - Cohort.Type
*/
with source_table as (
    select
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
        case
            when
                RaceEthFedRollup = 'Hispanic or Latino of any race'
                then 'Hispanic or Latino'
            when RaceEthFedRollup is null then 'Unknown/Missing'
            else RaceEthFedRollup
        end as RaceEthnicity,
        IsEll,
        EllStatus,
        HasFrl,
        FrlStatus,
        Has504Plan,
        HasIep,
        Email,
        IsCurrentlyEnrolled,
        CurrentSchoolId,
        CurrentNameOfInstitution as CurrentSchoolName,
        cast(CurrentGradeLevel as int64) as CurrentGradeLevel
    from {{ source('StarterPack', 'StudentDemographics') }}
    -- These are fake/test student accounts
    where StudentUniqueId not in ('16671', '16667', '16668')
),

students_with_iep as (
    select *
    from {{ ref('stg_RD__Seis') }}
    where
        StudentEligibilityStatus = 'Eligible/Previously Eligible'
        and SpedExitDate is null
),

school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

final as (
    select
        school_year.SchoolYear,
        source_table.* except (
            HasIep,
            Email,
            IsCurrentlyEnrolled,
            CurrentSchoolId,
            CurrentSchoolName,
            CurrentGradeLevel
        ),
        coalesce (iep.StudentEligibilityStatus = 'Eligible/Previously Eligible',
        false) as HasIep,
        iep.StudentEligibilityStatus as SeisEligibilityStatus,
        source_table.Email,
        source_table.IsCurrentlyEnrolled,
        source_table.CurrentSchoolId,
        source_table.CurrentSchoolName,
        source_table.CurrentGradeLevel
    from source_table
    cross join school_year
    left join students_with_iep as iep
        on source_table.StateUniqueId = iep.StateUniqueId
)

select * from final
