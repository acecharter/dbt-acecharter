SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  schoolname AS SchoolName,
  districtname AS DistrictName,
  countyname AS CountyName,
  charter_flag AS CharterFlag,
  CAST(coe_flag AS BOOL) AS CoeFlag,
  dass_flag AS DassFlag,
  studentgroup AS StudentGroup,
  currnumer AS CurrNumer,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  priornumer AS PriorNumer,
  priordenom AS PriorDenom,
  priorstatus AS PriorStatus,
  change AS Change,
  safetynet AS SafetyNet,
  statuslevel AS StatusLevel,
  changelevel AS ChangeLevel,
  color AS Color,
  box AS Box,
  certifyflag AS CertifyFlag,
  reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashChronic2018')}}
WHERE 
  rtype = 'X'
  OR rtype = 'D' AND cds = 43693690000000 --ARUSD
  OR rtype = 'D' AND cds = 43696660000000 --SJUSD
  OR rtype = 'D' AND cds = 43694500000000 --FMSD
  OR rtype = 'S' AND cds = 43693696046197 --Lee Mathson Middle
  OR rtype = 'S' AND cds = 43693690107763 --Renaissance Academy
  OR rtype = 'S' AND cds = 43696666062103 --Muwekma Ohlone Middle
  OR rtype = 'S' AND cds = 43694506062103 --Bridges Academy
  OR rtype = 'S' AND cds = 43104390116814 --ACE Empower
  OR rtype = 'S' AND cds = 43696660131656 --ACE Inspire
  OR rtype = 'S' AND cds = 43694500129247 --ACE Esperanza
