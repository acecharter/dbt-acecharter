-- No fields dropped from source table

SELECT
  SchoolId,
  NameOfInstitution,
  WeekOf,
  CAST(GradeLevel AS int64) AS GradeLevel,
  Month,
  MonthRank,
  EventDate,
  Apportionment,
  Possible
FROM {{ source('StarterPack', 'AverageDailyAttendance_v3')}}