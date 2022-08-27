SELECT
  SchoolYear,
  MIN(StartDate) AS StartDate,
  MAX(EndDate) AS EndDate
FROM {{ ref('stg_RD__AttendanceReportingPeriods')}}
GROUP BY SchoolYear