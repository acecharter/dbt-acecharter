SELECT
  MonthName,
  MonthNumber,
  DayRange,
  DayRangeStart,
  DayRangeEnd,
  CONCAT(
    CAST(GradePlacementTenthsPlaceValue AS STRING),
    CAST(GradePlacementHundredthsPlaceValue AS STRING)
  ) AS GradePlacementDecimalValue,
FROM {{ source('GoogleSheetData', 'RenStarGradePlacementByDayRange')}}