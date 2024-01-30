select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    coalesce(charter_flag, false) as CharterFlag,
    coalesce(coe_flag, false) as CoeFlag,
    coalesce(dass_flag, false) as DassFlag,
    type as Type,
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
    currnsizemet as CurrNSizeMet,
    priornsizemet as PriorNSizeMet,
    accountabilitymet as AccountabilityMet,
    coalesce (certifyflag = 'Y', false) as CertifyFlag,
    coalesce(priorcertifyflag, false) as PriorCertifyFlag,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashSusp2023') }}
where
    rtype = 'X'
    or (rtype = 'S' and charter_flag = true and dass_flag is null)
    -- or substr(cast(cds as string), 1, 7) in (
    --     '4369369',  -- ARUSD
    --     '4369666',  -- SJUSD (includes ACE Inspire)
    --     '4369450',  -- FMSD (includes ACE Esperanza)
    --     '4369427'  -- ESUHSD (includes ACE Charter High)
    -- )
    -- or cds = 43104390116814  -- ACE Empower
