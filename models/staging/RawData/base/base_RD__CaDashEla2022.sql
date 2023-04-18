SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    NULLIF(TRIM(schoolname),' ') AS SchoolName,
    districtname AS DistrictName,
    NULLIF(TRIM(countyname),'') AS CountyName,
    CASE WHEN charter_flag = 'Y' THEN TRUE ELSE FALSE END AS CharterFlag,
    CASE WHEN coe_flag = 'Y' THEN TRUE ELSE FALSE END AS CoeFlag,
    CASE WHEN dass_flag = 'Y' THEN TRUE ELSE FALSE END AS DassFlag,
    studentgroup AS StudentGroup,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    statuslevel AS StatusLevel,
    CAST(
        CASE
            WHEN hscutpoints = 'Y' THEN TRUE
            WHEN TRIM(hscutpoints) = '' THEN FALSE
        END
    AS BOOL) AS HsCutPoints,
    pairshare_method AS PairShareMethod,
    prate_enrolled AS PRateEnrolled,
    prate_tested AS PRateTested,
    prate AS PRate,
    numPRLOSS AS NumPrLoss,
    currdenom_withoutPRLOSS AS CurrDenomWithoutPrLoss,
    currstatus_withoutPRLOSS AS CurrStatusWithoutPrLoss,
    reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashEla2022')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower