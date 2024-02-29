select
    SchoolCode,
    SSID,
    StudentName,
    LocalID,
    RFEPDate
from {{ source('RawData', 'Calpads217SelaHighSchool2022') }}
where SchoolCode is not null
