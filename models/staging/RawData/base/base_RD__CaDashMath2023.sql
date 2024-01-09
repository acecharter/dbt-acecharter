select
    cast(cds as string) as Cds,
    rtype as RType,
    nullif(trim(schoolname), ' ') as SchoolName,
    districtname as DistrictName,
    nullif(trim(countyname), '') as CountyName,
    coalesce(charter_flag, false) as CharterFlag,
    coalesce(coe_flag, false) as CoeFlag,
    coalesce(dass_flag, false) as DassFlag,
    studentgroup as StudentGroup,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    priordenom as PriorDenom,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    box as Box,
    coalesce(hscutpoints, false) as HsCutPoints,
    pairshare_method as PairShareMethod,
    currprate_enrolled as CurrPRateEnrolled,
    currprate_tested as CurrPRateTested,
    currprate as CurrPRate,
    currnumPRLOSS as CurrNumPrLoss,
    currdenom_withoutPRLOSS as CurrDenomWithoutPrLoss,
    currstatus_withoutPRLOSS as CurrStatusWithoutPrLoss,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashMath2023') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
