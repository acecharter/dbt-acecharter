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
