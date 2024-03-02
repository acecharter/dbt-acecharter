select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217Elas2022HighSchool') }}
where SchoolCode is not null
