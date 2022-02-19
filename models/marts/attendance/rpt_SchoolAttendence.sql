WITH
  schools AS (
    SELECT
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ref('dim_Schools')}}
  ),

  ada AS (
    SELECT * FROM {{ ref('stg_SP__AverageDailyAttendance_v3') }}
  ),

  final AS (
    SELECT
      s.*,
      a.* EXCEPT(SchoolId),
      ROUND(a.Apportionment / a.Possible, 4) AS DailyAttendanceRate
    FROM ada AS a
    LEFT JOIN schools AS s
    USING(SchoolId)
  )

SELECT * FROM final