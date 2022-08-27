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
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentStarterPackSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.*
  FROM source_table
  CROSS JOIN sy
)

SELECT * FROM final