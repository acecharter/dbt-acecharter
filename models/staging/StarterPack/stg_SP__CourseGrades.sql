SELECT *
FROM {{ source('StarterPack', 'CourseGrades')}}
WHERE
  DATE(_PARTITIONTIME) = CURRENT_DATE('America/Los_Angeles')
  --DATE(_PARTITIONTIME) = '2022-06-15' --Update the date and use this line in lieu of the preceding line to keep grades dashboard updated over the summer
  AND LetterGradeEarned IS NOT NULL