SELECT
    Code AS ReportingCategoryCode,
    ReportingCategory,
    Type AS ReportingCategoryType
FROM {{ source('GoogleSheetData', 'CdeReportingCategories')}}