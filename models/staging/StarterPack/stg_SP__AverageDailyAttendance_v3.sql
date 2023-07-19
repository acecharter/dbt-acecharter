with source_table as (
    select
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
),

sy as (
    select * from {{ ref('dim_CurrentSchoolYear') }}
),

final as (
    select
        sy.SchoolYear,
        source_table.*
    from source_table
    cross join sy
)

select * from final
