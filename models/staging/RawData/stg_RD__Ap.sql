WITH final AS (
  SELECT * FROM {{ ref('base_RD__Ap2018')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RD__Ap2019')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RD__Ap2020')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RD__Ap2021')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RD__Ap2022')}}
)

SELECT * FROM final