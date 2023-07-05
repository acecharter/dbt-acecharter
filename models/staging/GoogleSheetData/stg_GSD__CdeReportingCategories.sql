select
    Code as ReportingCategoryCode,
    ReportingCategory,
    Type as ReportingCategoryType
from {{ source('GoogleSheetData', 'CdeReportingCategories') }}
