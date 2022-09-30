WITH source_table AS (
  SELECT * FROM {{ source('StarterPack', 'CourseGrades')}}
  WHERE
    DATE(_PARTITIONTIME) = CURRENT_DATE('America/Los_Angeles')
    --DATE(_PARTITIONTIME) = '2022-06-15' --Update the date and use this line in lieu of the preceding line to keep grades dashboard updated over the summer
    AND LetterGradeEarned IS NOT NULL
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