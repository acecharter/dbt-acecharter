select
    '2022-23' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CalendarDates_SY23') }}
