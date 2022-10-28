WITH final AS (
  SELECT * FROM {{ ref('base_RD__Anet2122')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RD__Anet2223')}}
)

SELECT * FROM final