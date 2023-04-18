WITH source_table AS(
    SELECT
        SchoolId,
        NameOfInstitution,
        StudentUniqueId,
        LastSurname,
        FirstName,
        ROUND(AverageDailyAttendance, 4) AS AverageDailyAttendance,
        CountOfAllAbsenceEvents AS CountOfDaysAbsent,
        CountOfAllInAttendanceEvents AS CountOfDaysInAttendance,
        CountOfDaysEnrolled
    FROM {{ source('StarterPack', 'StudentAttendance_v2')}}
    WHERE StudentUniqueId NOT IN ('16671', '16667', '16668') -- These are fake/test student accounts
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