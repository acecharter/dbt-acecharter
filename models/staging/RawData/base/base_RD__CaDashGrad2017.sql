SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    schoolname AS SchoolName,
    districtname AS DistrictName,
    countyname AS CountyName,
    charter_flag AS CharterFlag,
    CAST(coe_flag AS BOOL) AS CoeFlag,
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
    CAST(SUBSTR(reportingyear,1,4) AS INT64) AS ReportingYear
FROM {{ source('RawData', 'CaDashGrad2017')}}
WHERE 
    rtype = 'X'
    OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)