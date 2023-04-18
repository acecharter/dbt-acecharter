SELECT
    '2021-22' AS SchoolYear,
    *
FROM {{ source('StarterPack_Archive', 'CourseGrades_SY22')}}
WHERE
    DATE(_PARTITIONTIME) = '2022-06-15'
    AND LetterGradeEarned IS NOT NULL