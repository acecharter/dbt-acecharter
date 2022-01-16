WITH caaspp2021 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppSantaClaraCounty2021')}}
),
caaspp2019 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppSantaClaraCounty2019')}}
),
caaspp2018 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppSantaClaraCounty2018')}}
),
final AS (
  SELECT * FROM caaspp2021
  UNION ALL
  SELECT * FROM caaspp2019
  UNION ALL
  SELECT * FROM caaspp2018
)

SELECT * FROM final