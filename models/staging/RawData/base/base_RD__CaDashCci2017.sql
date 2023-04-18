SELECT
    CAST(cds AS STRING) AS Cds,
    rtype AS RType,
    schoolname AS SchoolName,
    districtname AS DistrictName,
    countyname AS CountyName,
    charter_flag AS CharterFlag,
    CAST(coe_flag AS BOOL) AS CoeFlag,
    studentgroup AS StudentGroup,
    studentgroup_pct AS StudentGroupPct,
    currdenom AS CurrDenom,
    currstatus AS CurrStatus,
    curr_prep AS CurrPrep,
    curr_prep_pct AS CurrPrepPct,
    statuslevel AS StatusLevel,
    CAST(SUBSTR(reportingyear,1,4) AS INT64) AS ReportingYear
FROM {{ source('RawData', 'CaDashCci2017')}}
WHERE 
    rtype = 'X'
    OR SUBSTR(CAST(cds AS STRING),1,7) = '4369427' --ESUHSD (includes ACE Charter High)