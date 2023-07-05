select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    cast(coe_flag as bool) as CoeFlag,
    studentgroup as StudentGroup,
    studentgroup_pct as StudentGroupPct,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    curr_prep as CurrPrep,
    curr_prep_pct as CurrPrepPct,
    statuslevel as StatusLevel,
    cast(substr(reportingyear, 1, 4) as int64) as ReportingYear
from {{ source('RawData', 'CaDashCci2017') }}
where
    rtype = 'X'
    --ESUHSD (includes ACE Charter High)
    or substr(cast(cds as string), 1, 7) = '4369427'
