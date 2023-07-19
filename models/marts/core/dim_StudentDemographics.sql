with demographics as (
    select * from {{ ref('stg_SP__StudentDemographics') }}
    union all
    select * from {{ ref('stg_SPA__StudentDemographics_SY22') }}
),

final as (
    select
        SchoolYear,
        StudentUniqueId,
        StateUniqueId,
        SisUniqueId,
        DisplayName,
        LastSurname as LastName,
        FirstName,
        MiddleName,
        Birthdate,
        BirthSex as Gender,
        RaceEthnicity,
        case when IsEll is true then 'Yes' else 'No' end as IsEll,
        EllStatus,
        case when HasFrl is true then 'Yes' else 'No' end as HasFrl,
        FrlStatus,
        case when HasIep is true then 'Yes' else 'No' end as HasIep,
        SeisEligibilityStatus,
        Email,
        IsCurrentlyEnrolled
    from demographics
)

select * from final
