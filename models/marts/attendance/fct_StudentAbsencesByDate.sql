WITH stu_att_by_date AS (
  SELECT * FROM {{ ref('stg_SP__StudentAttendanceByDate')}}
  UNION ALL
  SELECT * FROM {{ ref('stg_SPA__StudentAttendanceByDate_SY22')}}
),

final AS (
  SELECT *
  FROM unioned
  WHERE AttendanceEventCategoryDescriptor = 'Absent'
)

SELECT * FROM final
