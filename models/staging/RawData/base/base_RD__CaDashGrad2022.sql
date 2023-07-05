select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    coe_flag as CoeFlag,
    dass_flag as DassFlag,
    studentgroup as StudentGroup,
    currnumer as CurrNumer,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    fiveyrnumer as FiveYrNumer,
    statuslevel as StatusLevel,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashGrad2022') }}
where
    rtype = 'X'
    --ESUHSD (includes ACE Charter High)
    or substr(cast(cds as string), 1, 7) = '4369427'
