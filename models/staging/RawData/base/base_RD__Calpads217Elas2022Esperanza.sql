select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217Elas2022Esperanza') }}
where SchoolCode is not null
