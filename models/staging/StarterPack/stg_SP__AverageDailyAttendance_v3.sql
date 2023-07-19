select
    case
        when extract(month from WeekOf) > 7
            then concat(
                extract(year from WeekOf),
                '-',
                substr(cast((extract(year from WeekOf) + 1) as string), 3, 2)
            )
        when extract(month from WeekOf) <= 7
            then concat(
                extract(year from WeekOf) - 1,
                '-',
                extract(year from WeekOf) - 2000
            )
        else 'ERROR'
    end as SchoolYear,
    SchoolId,
    NameOfInstitution,
    WeekOf,
    cast(GradeLevel as int64) as GradeLevel,
    Month,
    MonthRank,
    EventDate,
    Apportionment,
    Possible
from {{ source('StarterPack', 'AverageDailyAttendance_v3') }}
