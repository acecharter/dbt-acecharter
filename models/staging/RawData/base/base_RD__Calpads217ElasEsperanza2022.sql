select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217ElasEsperanza2022') }}
where SchoolCode is not null
