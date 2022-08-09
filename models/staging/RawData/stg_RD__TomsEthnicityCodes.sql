SELECT
  CAST(EthnicityCode AS STRING) AS RaceEthnicityCode,
  Ethnicity AS RaceEthnicity
FROM {{ source('RawData', 'TomsEthnicityCodes')}}