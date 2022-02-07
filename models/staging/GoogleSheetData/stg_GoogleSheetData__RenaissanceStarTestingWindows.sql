SELECT
  ID AS TestingWindowId,
  SchoolYear,
  TestingWindowType,
  TestingWindowName,
  DATE(StartDate) AS TestingWindowStartDate,
  DATE(EndDate)AS TestingWindowEndDate
FROM {{ source('GoogleSheetData', 'RenaissanceStarTestingWindows')}}