select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    coe_flag as CoeFlag,
    dass_flag as DassFlag,
    currprogressed as CurrProgressed,
    pctcurrprogressed as PctCurrProgressed,
    currmaintainPL4 as CurrMaintainPL4,
    pctcurrmaintainPL4 as PctCurrMaintainPL4,
    currmaintainoth as CurrMaintainOth,
    pctcurrmaintainoth as PctCurrMaintainOth,
    currdeclined as CurrDeclined,
    currnumer as CurrNumer,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    statuslevel as StatusLevel,
    flag95pct as Flag95Pct,
    nsizemet as NSizeMet,
    nsizegroup as NSizeGroup,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashElpi2022') }}
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
