select
    SchoolYear,
    TestingWindow,
    date(StartDate) as TestingWindowStartDate,
    date(EndDate) as TestingWindowEndDate,
    date(AceWindowStartDate) as AceWindowStartDate,
    date(AceWindowEndDate) as AceWindowEndDate,
    date(EligibleStudentsEnrollmentDate) as EligibleStudentsEnrollmentDate
from {{ source('GoogleSheetData', 'RenStarTestingWindows') }}
