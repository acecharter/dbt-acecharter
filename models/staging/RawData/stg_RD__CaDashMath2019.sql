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
  currstatus AS CurrStatus,
  priordenom AS PriorDenom,
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
  notestflag AS NoTestFlag,
  ReportingYear
FROM {{ source('RawData', 'CaDashMath2019')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369369' --ARUSD
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369666' --SJUSD (includes ACE Inspire)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369450' --FMSD (includes ACE Esperanza)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)
  OR cds = 43104390116814 --ACE Empower
