SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    schoolname AS SchoolName,
    districtname AS DistrictName,
    countyname AS CountyName,
    charter_flag AS CharterFlag,
    coe_flag AS CoeFlag,
    dass_flag AS DassFlag,
    studentgroup AS StudentGroup,
    currdenom AS CurrDenom,
    currdenom_swd AS CurrDenomSwd,
    currstatus AS CurrStatus,
    priordenom AS PriorDenom,
    priordenom_swd AS PriorDenom_Swd,
    priorstatus AS PriorStatus,
    change AS Change,
    statuslevel AS StatusLevel,
    changelevel AS ChangeLevel,
    color AS Color,
    box AS Box,
    hscutpoints AS HsCutPoints,
    curradjustment AS CurrAdjustment,
    prioradjustment AS PriorAdjustment,
    pairshare_method AS PairShareMethod,
    caa_denom AS CaaDenom,
    caa_level1_num AS CaaLevel1Num,
    caa_level1_pct AS CaaLevel1Pct,
    caa_level2_num AS CaaLevel2Num,
    caa_level2_pct AS CaaLevel2Pct,
    caa_level3_num AS CaaLevel3Num,
    caa_level3_pct AS CaaLevel3_Pct,
    reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashEla2018')}}
WHERE
    rtype = 'X'
    or SUBSTR(CAST(cds as STRING), 1, 7) in (
        '4369369',  -- ARUSD
        '4369666',  -- SJUSD (includes ACE Inspire)
        '4369450',  -- FMSD (includes ACE Esperanza)
        '4369427'  -- ESUHSD (includes ACE Charter High)
    )
    or cds = 43104390116814  -- ACE Empower