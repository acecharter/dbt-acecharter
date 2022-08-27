WITH unioned AS (
  SELECT * FROM {{ ref('stg_SP__AverageDailyAttendance_v3') }}
  UNION ALL
  SELECT * FROM {{ ref('stg_SPA__AverageDailyAttendance_v3_SY22')}}
),

final AS (
  SELECT * EXCEPT(NameOfInstitution)
  FROM unioned
)

SELECT * FROM final