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
    currmaintainPL4 as CurrMaintainPL4,
    currmaintainoth as CurrMaintainOth,
    currdeclined as CurrDeclined,
    currprogressed_Alternate as CurrProgressedAlternate,
    currmaintainPL3_Alternate as CurrMaintainPl3Alternate,
    currnotprognotmain_Alternate as CurrNotProgNotMainAlternate,
    curr95 as Curr95,
    currnumer as CurrNumer,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    priorprogressed as PriorProgressed,
    priormaintainPL4 as PriorMaintainPL4,
    priormaintainoth as PriorMaintainOth,
    priordeclined as PriorDeclined,
    prior95 as Prior95,
    priornumer as PriorNumer,
    priordenom as PriorDenom,
    priorstatus as PriorStatus,
    change as Change,
    statuslevel as StatusLevel,
    changelevel as ChangeLevel,
    color as Color,
    box as Box,
    currnsizemet as CurrNSizeMet,
    currnsizegroup as CurrNSizeGroup,
    priornsizemet as PriorNSizeMet,
    priornsizegroup as PriorNSizeGroup,
    accountabilitymet as AccountabilityMet,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashElpi2023') }}
where
    rtype = 'X'
    or substr(cast(cds as string), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
