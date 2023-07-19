select
    '2021-22' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CourseGrades_SY22') }}
where
    date(_PARTITIONTIME) = '2022-06-15'
    and LetterGradeEarned is not null
