select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    coe_flag as CoeFlag,
    dass_flag as DassFlag,
    studentgroup as StudentGroup,
    currnumer as CurrNumer,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    priornumer as PriorNumer,
    priordenom as PriorDenom,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    box as Box,
    smalldenom as SmallDenom,
    fiveyrnumer as FiveYrNumer,
    currnsizemet as CurrNSizeMet,
    priornsizemet as PriorNSizeMet,
    accountabilitymet as AccountabilityMet,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashGrad2023') }}
where
    rtype = 'X'
    or (rtype = 'S' and charter_flag = true and dass_flag is null)
    -- --ESUHSD (includes ACE Charter High)
    -- or substr(cast(cds as string), 1, 7) = '4369427'
