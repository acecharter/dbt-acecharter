select
    '2021-22' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'Schools_SY22') }}
