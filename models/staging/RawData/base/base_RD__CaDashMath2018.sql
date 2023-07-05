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
    currdenom as CurrDenom,
    currdenom_swd as CurrDenomSwd,
    currstatus as CurrStatus,
    priordenom as PriorDenom,
    priordenom_swd as PriorDenom_Swd,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    box as Box,
    hscutpoints as HsCutPoints,
    curradjustment as CurrAdjustment,
    prioradjustment as PriorAdjustment,
    pairshare_method as PairShareMethod,
    caa_denom as CaaDenom,
    caa_level1_num as CaaLevel1Num,
    caa_level1_pct as CaaLevel1Pct,
    caa_level2_num as CaaLevel2Num,
    caa_level2_pct as CaaLevel2Pct,
    caa_level3_num as CaaLevel3Num,
    caa_level3_pct as CaaLevel3_Pct,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashMath2018') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
