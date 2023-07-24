select
    '2022-23' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'Schools_SY23') }}
