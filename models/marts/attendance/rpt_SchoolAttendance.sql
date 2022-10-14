WITH
  schools AS (
    SELECT
      SchoolYear,
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ref('dim_Schools')}}
  ),

  ada AS (
    SELECT * FROM {{ ref('fct_SchoolAverageDailyAttendance')}}
  ),

  reporting_periods AS (
    SELECT
      a.EventDate,
      p.ReportingPeriod
    FROM ada AS a
    CROSS JOIN {{ ref('stg_RD__AttendanceReportingPeriods')}} AS p
    WHERE a.EventDate BETWEEN p.StartDate AND p.EndDate
    GROUP BY 1, 2
    ORDER BY a.EventDate
  ),

  final AS (
    SELECT
      s.*,
      a.GradeLevel,
      p.ReportingPeriod,
      a.Month,
      a.MonthRank,
      a.WeekOf,
      a.EventDate,
      a.Apportionment,
      a.Possible,
      ROUND(a.Apportionment / a.Possible, 4) AS DailyAttendanceRate
    FROM ada AS a
    LEFT JOIN schools AS s
    ON a.SchoolId = s.SchoolId
    AND a.SchoolYear = s.SchoolYear
    LEFT JOIN reporting_periods AS p
    ON a.EventDate = p.EventDate
  )

SELECT * FROM final