select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217SelaEsperanza2023') }}
where SchoolCode is not null
