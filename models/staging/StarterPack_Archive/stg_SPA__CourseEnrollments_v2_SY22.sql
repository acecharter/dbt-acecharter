select
    '2021-22' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CourseEnrollments_v2_SY22') }}
