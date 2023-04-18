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
    currmaintainPL4 AS CurrMaintainPL4,
    currdeclined AS CurrDeclined,
    currnumer AS CurrNumer,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    statuslevel AS StatusLevel,
    flag95pct AS Flag95Pct,
    nsizemet AS NSizeMet,
    nsizegroup AS NSizeGroup,
    reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashElpi2019')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower