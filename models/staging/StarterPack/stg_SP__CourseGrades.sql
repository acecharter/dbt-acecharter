SELECT *
FROM {{ source('StarterPack', 'CourseGrades')}}
WHERE
  --DATE(_PARTITIONTIME) = CURRENT_DATE('America/Los_Angeles')
  DATE(_PARTITIONTIME) = '2022-06-15' --This is temporary code until the start of the 22-23 school year
  AND LetterGradeEarned IS NOT NULL