SELECT
  CAST(EthnicityCode AS STRING) AS EthnicityCode,
  Ethnicity
FROM {{ source('RawData', 'TomsEthnicityCodes')}}