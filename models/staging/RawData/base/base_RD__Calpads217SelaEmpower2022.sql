select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217SelaEmpower2022') }}
where SchoolCode is not null
