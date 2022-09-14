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

  enrollment AS (
    SELECT * FROM {{ref('fct_SchoolEnrollmentByDate')}}
  ),

  reporting_periods AS (
    SELECT
      e.CalendarDate,
      p.ReportingPeriod
    FROM enrollment AS e
    CROSS JOIN {{ ref('stg_RD__AttendanceReportingPeriods')}} AS p
    WHERE e.CalendarDate BETWEEN p.StartDate AND p.EndDate
    GROUP BY 1, 2
    ORDER BY e.CalendarDate
  ),

  final AS (
    SELECT
      s.*,
      p.ReportingPeriod,
      e.* EXCEPT(SchoolId, SchoolYear)
    FROM schools AS s
    RIGHT JOIN enrollment AS e
    ON s.SchoolId = e.SchoolId
    AND s.SchoolYear = e.SchoolYear
    LEFT JOIN reporting_periods AS p
    ON e.CalendarDate = p.CalendarDate
    ORDER BY s.SchoolName, e.CalendarDate
  )

SELECT * FROM final
