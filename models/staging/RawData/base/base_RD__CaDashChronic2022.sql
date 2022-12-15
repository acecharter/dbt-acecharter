SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  NULLIF(schoolname,'') AS SchoolName,
  districtname AS DistrictName,
  NULLIF(countyname,'') AS CountyName,
  CAST(NULLIF(charter_flag,'') AS BOOL) AS CharterFlag,
  CAST(NULLIF(coe_flag,'') AS BOOL) AS CoeFlag,
  CAST(NULLIF(dass_flag,'') AS BOOL) AS DassFlag,
  studentgroup AS StudentGroup,
  currnumer AS CurrNumer,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  statuslevel AS StatusLevel,
  CAST(NULLIF(certifyflag,'') AS BOOL) AS CertifyFlag,
  CAST(NULLIF(dataerrorflag,'') AS BOOL) AS DataErrorFlag,
  reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashChronic2022')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369369' --ARUSD
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369666' --SJUSD (includes ACE Inspire)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369450' --FMSD (includes ACE Esperanza)
  OR cds = 43104390116814 --ACE Empower