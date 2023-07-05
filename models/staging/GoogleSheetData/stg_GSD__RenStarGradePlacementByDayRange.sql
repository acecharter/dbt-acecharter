select
    MonthName,
    MonthNumber,
    DayRange,
    DayRangeStart,
    DayRangeEnd,
    concat(
        cast(GradePlacementTenthsPlaceValue as string),
        cast(GradePlacementHundredthsPlaceValue as string)
    ) as GradePlacementDecimalValue
from {{ source('GoogleSheetData', 'RenStarGradePlacementByDayRange') }}
