select
    concat(
        SchoolYear,
        ' ',
        left(ReportingPeriodType, 1),
        cast(ReportingPeriodNumber as string)
    ) as AttendanceReportingPeriodUniqueId,
    SchoolYear,
    ReportingPeriodType,
    ReportingPeriodNumber,
    concat(
        left(ReportingPeriodType, 1), cast(ReportingPeriodNumber as string)
    ) as ReportingPeriod,
    date(StartDate) as StartDate,
    date(EndDate) as EndDate
from {{ source('RawData', 'AttendanceReportingPeriods') }}
