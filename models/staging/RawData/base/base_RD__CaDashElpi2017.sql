SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  schoolname AS SchoolName,
  districtname AS DistrictName,
  countyname AS CountyName,
  charter_flag AS CharterFlag,
  coe_flag AS CoeFlag,
  curradvanced AS CurrProgressed,
  currmaintained AS CurrMaintainPL4,
  currnumer AS CurrNumer,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  priornumer AS PriorNumer,
  priordenom AS PriorDenom,
  priorstatus AS PriorStatus,
  change AS Change,
  statuslevel AS StatusLevel,
  changeLevel AS ChangeLevel,
  color AS Color,
  box AS Box,
  flag50pct AS Flag50Pct,
  nsize_met AS NSizeMet,
  CAST(SUBSTR(reportingyear,1,4) AS INT64) AS ReportingYear
FROM {{ source('RawData', 'CaDashElpi2017')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369369' --ARUSD
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369666' --SJUSD (includes ACE Inspire)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369450' --FMSD (includes ACE Esperanza)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)
  OR cds = 43104390116814 --ACE Empower