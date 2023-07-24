select
    '2022-23' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CourseEnrollments_v2_SY23') }}
