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
  currnumer AS CurrNumer,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  priornumer AS PriorNumer,
  priordenom AS PriorDenom,
  priorstatus AS PriorStatus,
  safetynet AS SafetyNet,
  change AS Change,
  statuslevel AS StatusLevel,
  changelevel AS ChangeLevel,
  color AS Color,
  box AS Box,
  reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashGrad2018')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)