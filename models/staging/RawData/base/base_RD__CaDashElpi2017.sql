select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    cast(coe_flag as bool) as CoeFlag,
    curradvanced as CurrProgressed,
    currmaintained as CurrMaintainPL4,
    currnumer as CurrNumer,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    priornumer as PriorNumer,
    priordenom as PriorDenom,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changeLevel as ChangeLevel,
    color as Color,
    box as Box,
    flag50pct as Flag50Pct,
    nsize_met as NSizeMet,
    cast(substr(reportingyear, 1, 4) as int64) as ReportingYear
from {{ source('RawData', 'CaDashElpi2017') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
