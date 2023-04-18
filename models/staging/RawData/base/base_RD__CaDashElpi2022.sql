SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    schoolname AS SchoolName,
    districtname AS DistrictName,
    countyname AS CountyName,
    charter_flag AS CharterFlag,
    coe_flag AS CoeFlag,
    dass_flag AS DassFlag,
    currprogressed AS CurrProgressed,
    pctcurrprogressed AS PctCurrProgressed,
    currmaintainPL4 AS CurrMaintainPL4,
    pctcurrmaintainPL4 AS PctCurrMaintainPL4,
    currmaintainoth AS CurrMaintainOth,
    pctcurrmaintainoth AS PctCurrMaintainOth,
    currdeclined AS CurrDeclined,
    currnumer AS CurrNumer,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    statuslevel AS StatusLevel,
    flag95pct AS Flag95Pct,
    nsizemet AS NSizeMet,
    nsizegroup AS NSizeGroup,
    reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashElpi2022')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower