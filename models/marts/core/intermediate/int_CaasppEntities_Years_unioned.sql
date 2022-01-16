WITH entities2021 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppEntities2021')}}
),
entities2019 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppEntities2019')}}
),
entities2018 AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppEntities2018')}}
),
final AS (
  SELECT * FROM entities2021
  UNION ALL
  SELECT * FROM entities2019
  UNION ALL
  SELECT * FROM entities2018
)

SELECT * FROM final