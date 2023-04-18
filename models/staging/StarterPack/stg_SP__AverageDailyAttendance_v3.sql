WITH
source_table AS (
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
),

sy AS (
    SELECT * FROM {{ ref('dim_CurrentSchoolYear')}}
),

final AS (
    SELECT
        sy.SchoolYear,
        source_table.*
    FROM source_table
    CROSS JOIN sy
)

SELECT * FROM final