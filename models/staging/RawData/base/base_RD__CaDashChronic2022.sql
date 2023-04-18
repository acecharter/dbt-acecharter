SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    NULLIF(schoolname,'') AS SchoolName,
    districtname AS DistrictName,
    NULLIF(countyname,'') AS CountyName,
    CASE WHEN charter_flag = 'Y' THEN TRUE ELSE FALSE END AS CharterFlag,
    CASE WHEN coe_flag = 'Y' THEN TRUE ELSE FALSE END AS CoeFlag,
    CASE WHEN dass_flag = 'Y' THEN TRUE ELSE FALSE END AS DassFlag,
    studentgroup AS StudentGroup,
    currnumer AS CurrNumer,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    statuslevel AS StatusLevel,
    CASE WHEN certifyflag = 'Y' THEN TRUE ELSE FALSE END AS CertifyFlag,
    CASE WHEN dataerrorflag = 'Y' THEN TRUE ELSE FALSE END AS DataErrorFlag,
    reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashChronic2022')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450'  -- FMSD (includes ACE Esperanza)
    )
    or cds = 43104390116814  -- ACE Empower