WITH source_table AS (
  SELECT
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    EventDate,
    CASE WHEN AttendanceEventCategoryDescriptor = 'Unreconciled' THEN 'Absent' ELSE NULL END AS AttendanceEventCategoryDescriptor,
    EventDuration
  FROM {{ source('StarterPack', 'StudentAttendanceByDate')}}
  WHERE StudentUniqueId NOT IN ('16671', '16667', '16668')  -- These are fake/test student accounts
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