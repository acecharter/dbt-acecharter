SELECT
    '2021-22' AS SchoolYear,
    SchoolId,
    NameOfInstitution,
    WeekOf,
    CAST(GradeLevel AS int64) AS GradeLevel,
    Month,
    MonthRank,
    EventDate,
    Apportionment,
    Possible
FROM {{ source('StarterPack_Archive', 'AverageDailyAttendance_v3_SY22')}}