SELECT
  SchoolYear,
  TestingWindow,
  DATE(StartDate) AS TestingWindowStartDate,
  DATE(EndDate) AS TestingWindowEndDate,
  DATE(AceWindowStartDate) AS AceWindowStartDate,
  DATE(AceWindowEndDate) AS AceWindowEndDate,
  DATE(EligibleStudentsEnrollmentDate) AS EligibleStudentsEnrollmentDate
FROM {{ source('GoogleSheetData', 'RenStarTestingWindows')}}