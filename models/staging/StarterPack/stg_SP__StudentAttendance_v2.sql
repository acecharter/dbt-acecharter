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