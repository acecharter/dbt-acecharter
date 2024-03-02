select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217Elas2023HighSchool') }}
where SchoolCode is not null
