select
    '2022-23' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CourseGrades_SY23') }}
where
    date(_PARTITIONTIME) = '2023-06-15'
    and LetterGradeEarned is not null
