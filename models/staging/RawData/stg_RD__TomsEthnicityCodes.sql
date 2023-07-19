select
    cast(EthnicityCode as string) as RaceEthnicityCode,
    Ethnicity as RaceEthnicity
from {{ source('RawData', 'TomsEthnicityCodes') }}
