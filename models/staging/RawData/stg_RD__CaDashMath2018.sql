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
FROM {{ source('RawData', 'CaDashMath2018')}}
WHERE 
    rtype = 'X'
    OR rtype = 'D' AND cds = 43693690000000 --ARUSD
    OR rtype = 'D' AND cds = 43696660000000 --SJUSD
    OR rtype = 'D' AND cds = 43694500000000 --FMSD
    OR rtype = 'D' AND cds = 43694270000000 --ESUHSD
    OR rtype = 'S' AND cds = 43693696046197 --Lee Mathson Middle
    OR rtype = 'S' AND cds = 43693690107763 --Renaissance Academy
    OR rtype = 'S' AND cds = 43696666062103 --Muwekma Ohlone Middle
    OR rtype = 'S' AND cds = 43694506062103 --Bridges Academy
    OR rtype = 'S' AND cds = 43694274330031 --Independence HS
    OR rtype = 'S' AND cds = 43104390116814 --ACE Empower
    OR rtype = 'S' AND cds = 43696660131656 --ACE Inspire
    OR rtype = 'S' AND cds = 43694500129247 --ACE Esperanza
    OR rtype = 'S' AND cds = 43694270125617 --ACE Charter HS
    