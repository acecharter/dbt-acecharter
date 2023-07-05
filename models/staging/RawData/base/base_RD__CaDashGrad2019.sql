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
    fiveyrnumer as FiveYrNumer,
    safetynet as SafetyNet,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    box as Box,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashGrad2019') }}
where
    rtype = 'X'
    --ESUHSD (includes ACE Charter High)
    or substr(cast(cds as string), 1, 7) = '4369427'
