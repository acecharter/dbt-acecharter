SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    schoolname AS SchoolName,
    districtname AS DistrictName,
    countyname AS CountyName,
    charter_flag AS CharterFlag,
    CAST(coe_flag AS BOOL) AS CoeFlag,
    type AS Type,
    studentgroup AS StudentGroup,
    currnumer AS CurrNumer,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    priornumer AS PriorNumer,
    priordenom AS PriorDenom,
    priorstatus AS PriorStatus,
    safetynet AS SafetyNet,
    change AS Change,
    statuslevel AS StatusLevel,
    changelevel AS ChangeLevel,
    color AS Color,
    box AS Box,
    CAST(certifyflag AS BOOL) AS CertifyFlag,
    CAST(SUBSTR(reportingyear,1,4) AS INT64) AS ReportingYear
FROM {{ source('RawData', 'CaDashSusp2017')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower
