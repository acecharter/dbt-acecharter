SELECT
  CONCAT(SchoolYear, ' ',LEFT(ReportingPeriodType, 1), CAST(ReportingPeriodNumber AS STRING)) AS AttendanceReportingPeriodUniqueId,
  SchoolYear,
  ReportingPeriodType,
  ReportingPeriodNumber,
  CONCAT(LEFT(ReportingPeriodType, 1), CAST(ReportingPeriodNumber AS STRING)) AS ReportingPeriod,
  DATE(StartDate) AS StartDate,
  DATE(EndDate) AS EndDate
FROM {{ source('RawData', 'AttendanceReportingPeriods')}}