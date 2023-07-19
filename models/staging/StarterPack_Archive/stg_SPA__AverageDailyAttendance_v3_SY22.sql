select
    '2021-22' as SchoolYear,
    SchoolId,
    NameOfInstitution,
    WeekOf,
    cast(GradeLevel as int64) as GradeLevel,
    Month,
    MonthRank,
    EventDate,
    Apportionment,
    Possible
from {{ source('StarterPack_Archive', 'AverageDailyAttendance_v3_SY22') }}
