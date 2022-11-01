SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  schoolname AS SchoolName,
  districtname AS DistrictName,
  countyname AS CountyName,
  charter_flag AS CharterFlag,
  CAST(coe_flag AS BOOL) AS CoeFlag,
  studentgroup AS StudentGroup,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  priordenom AS PriorDenom,
  priorstatus AS PriorStatus,
  change AS Change,
  statuslevel AS StatusLevel,
  changelevel AS ChangeLevel,
  color AS Color,
  caa_denom AS CaaDenom,
  caa_level1_num AS CaaLevel1Num,
  caa_level1_pct AS CaaLevel1Pct,
  caa_level2_num AS CaaLevel2Num,
  caa_level2_pct AS CaaLevel2Pct,
  caa_level3_num AS CaaLevel3Num,
  caa_level3_pct AS CaaLevel3_Pct,
  CAST(SUBSTR(reportingyear,1,4) AS INT64) AS ReportingYear
FROM {{ source('RawData', 'CaDashEla2017')}}
WHERE 
  rtype = 'X'
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369369' --ARUSD
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369666' --SJUSD (includes ACE Inspire)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369450' --FMSD (includes ACE Esperanza)
  OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)
  OR cds = 43104390116814 --ACE Empower