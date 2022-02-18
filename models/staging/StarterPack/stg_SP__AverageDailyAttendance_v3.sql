-- No fields dropped from source table

SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  WeekOf,
  CAST(GradeLevel AS int64) AS GradeLevel,
  Month,
  MonthRank,
  EventDate,
  Apportionment,
  Possible
FROM {{ source('StarterPack', 'AverageDailyAttendance_v3')}}