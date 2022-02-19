WITH schools AS (
  SELECT * FROM {{ref('dim_Schools')}}
),

ada AS (
    SELECT * FROM {{ ref('stg_SP__AverageDailyAttendance_v3') }}
)

SELECT
  a.SchoolId,
  s.SchoolName,
  s.SchoolNameShort,
  a.* EXCEPT(SchoolId, SchoolName),
  ROUND(a.Apportionment / a.Possible, 4) AS DailyAttendanceRate
FROM ada AS a
LEFT JOIN schools AS s
USING(SchoolId)