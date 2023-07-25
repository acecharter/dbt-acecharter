with starter_pack_schools as (
    select * from {{ ref('stg_SP__Schools') }}
    union all
    select * from {{ ref('stg_SPA__Schools_SY23') }}
    union all
    select * from {{ ref('stg_SPA__Schools_SY22') }}
),

gsd_schools as (
    select *
    from {{ ref('stg_GSD__Schools') }}
),

final as (
    select
        sp.SchoolYear,
        sp.SchoolId,
        gsd.StateCdsCode,
        gsd.StateCountyCode,
        gsd.StateDistrictCode,
        gsd.StateSchoolCode,
        gsd.SchoolNameFull as SchoolName,
        gsd.SchoolNameMid,
        gsd.SchoolNameShort,
        gsd.SchoolType,
        sp.PhysicalStreetNumberName,
        sp.PhysicalCity,
        sp.PhysicalStateAbbreviation,
        sp.PhysicalPostalCode,
        sp.MailingStreetNumberName,
        sp.MailingCity,
        sp.MailingStateAbbreviation,
        sp.MailingPostalCode,
        gsd.CurrentCharterTermStartDate,
        gsd.CurrentCharterTermEndDate,
        gsd.YearOpened,
        gsd.PreviousRenewalYears,
        gsd.GradesServed,
        gsd.Grade5,
        gsd.Grade6,
        gsd.Grade7,
        gsd.Grade8,
        gsd.Grade9,
        gsd.Grade10,
        gsd.Grade11,
        gsd.Grade12
    from starter_pack_schools as sp
    left join gsd_schools as gsd
        on sp.SchoolId = gsd.SchoolId
)

select distinct *
from final
order by SchoolYear desc
