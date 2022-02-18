SELECT *
FROM {{ source('StarterPack', 'CourseGrades')}}
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE('America/Los_Angeles')