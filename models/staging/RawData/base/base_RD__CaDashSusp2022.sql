SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  schoolname AS SchoolName,
  districtname AS DistrictName,
  countyname AS CountyName,
  CASE WHEN charter_flag = 'Y' THEN TRUE ELSE FALSE END AS CharterFlag,
  CASE WHEN coe_flag = 'Y' THEN TRUE ELSE FALSE END AS CoeFlag,
  CASE WHEN dass_flag = 'Y' THEN TRUE ELSE FALSE END AS DassFlag,
  type AS Type,
  studentgroup AS StudentGroup,
  currnumer AS CurrNumer,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  statuslevel AS StatusLevel,
  CASE WHEN certifyflag = 'Y' THEN TRUE ELSE FALSE END AS CertifyFlag,
  reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashSusp2022')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369369' --ARUSD
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369666' --SJUSD (includes ACE Inspire)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369450' --FMSD (includes ACE Esperanza)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)
  OR cds = 43104390116814 --ACE Empower
