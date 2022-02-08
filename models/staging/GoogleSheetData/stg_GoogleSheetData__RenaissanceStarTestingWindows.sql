SELECT
  ID AS TestingWindowId,
  SchoolYear,
  TestingWindowType,
  TestingWindowName,
  DATE(StartDate) AS TestingWindowStartDate,
  DATE(EndDate) AS TestingWindowEndDate,
  DATE(EnrollmentCutoffDate) AS EnrollmentEligibilityCutoffDate
FROM {{ source('GoogleSheetData', 'RenaissanceStarTestingWindows')}}