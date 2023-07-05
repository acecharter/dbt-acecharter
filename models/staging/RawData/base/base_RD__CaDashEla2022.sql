select
    cast(cds as string) as Cds,
    rtype as RType,
    nullif(trim(schoolname), ' ') as SchoolName,
    districtname as DistrictName,
    nullif(trim(countyname), '') as CountyName,
    coalesce (charter_flag = 'Y', false) as CharterFlag,
    coalesce (coe_flag = 'Y', false) as CoeFlag,
    coalesce (dass_flag = 'Y', false) as DassFlag,
    studentgroup as StudentGroup,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    statuslevel as StatusLevel,
    cast(
        case
            when hscutpoints = 'Y' then true
            when trim(hscutpoints) = '' then false
        end
        as bool
    ) as HsCutPoints,
    pairshare_method as PairShareMethod,
    prate_enrolled as PRateEnrolled,
    prate_tested as PRateTested,
    prate as PRate,
    numPRLOSS as NumPrLoss,
    currdenom_withoutPRLOSS as CurrDenomWithoutPrLoss,
    currstatus_withoutPRLOSS as CurrStatusWithoutPrLoss,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashEla2022') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
