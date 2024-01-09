select
    cast(cds as string) as Cds,
    rtype as RType,
    nullif(schoolname, '') as SchoolName,
    districtname as DistrictName,
    nullif(countyname, '') as CountyName,
    coalesce (charter_flag, false) as CharterFlag,
    coalesce (coe_flag, false) as CoeFlag,
    coalesce (dass_flag, false) as DassFlag,
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
    case when certifyflag = 'Y' then true else false end as CertifyFlag,
    case when dataerrorflag = 'Y' then true else false end as DataErrorFlag,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashChronic2023') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450'  -- FMSD (includes ACE Esperanza)
    )
    or cds = 43104390116814  -- ACE Empower
