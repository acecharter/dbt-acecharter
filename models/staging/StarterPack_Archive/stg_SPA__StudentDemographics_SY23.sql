select
    '2022-23' as SchoolYear,
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
    case when HasIep is true then 'Eligible/Previously Eligible' end
        as SeisEligibilityStatus,
    --Email addresses in 2021-22 were incorrect due to a data error
    cast(null as string) as Email,
    false as IsCurrentlyEnrolled,
    CurrentSchoolId,
    CurrentNameOfInstitution,
    cast(CurrentGradeLevel as int64) as CurrentGradeLevel
from {{ source('StarterPack_Archive', 'StudentDemographics_SY23') }}
--Exclude these fake/test students showing up in PS
where StudentUniqueId not in ('16671', '16667', '16668')
