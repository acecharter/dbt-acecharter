select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    cast(coe_flag as bool) as CoeFlag,
    studentgroup as StudentGroup,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    priordenom as PriorDenom,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    caa_denom as CaaDenom,
    caa_level1_num as CaaLevel1Num,
    caa_level1_pct as CaaLevel1Pct,
    caa_level2_num as CaaLevel2Num,
    caa_level2_pct as CaaLevel2Pct,
    caa_level3_num as CaaLevel3Num,
    caa_level3_pct as CaaLevel3_Pct,
    cast(substr(reportingyear, 1, 4) as int64) as ReportingYear
from {{ source('RawData', 'CaDashEla2017') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
