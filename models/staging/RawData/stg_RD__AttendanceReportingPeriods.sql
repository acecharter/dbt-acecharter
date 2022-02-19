SELECT
  CONCAT(SchoolYear, ' ',LEFT(ReportingPeriodType, 1),CAST(ReportingPeriodNumber AS STRING)) AS ReportingPeriodId,
  SchoolYear,
  ReportingPeriodType,
  ReportingPeriodNumber,
  CONCAT(LEFT(ReportingPeriodType, 1),CAST(ReportingPeriodNumber AS STRING)) AS ReportingPeriodShort,
  DATE(StartDate) AS StartDate,
  DATE(EndDate) AS EndDate
FROM {{ source('RawData', 'AttendanceReportingPeriods')}}