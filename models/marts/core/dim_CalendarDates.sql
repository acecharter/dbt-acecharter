WITH final AS (
  SELECT * FROM {{ ref('stg_SP__CalendarDates')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_SPA__CalendarDates_SY22')}}
)

SELECT * FROM final
