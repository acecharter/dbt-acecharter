select
    '2021-22' as SchoolYear,
    *
from {{ source('StarterPack_Archive', 'CalendarDates_SY22') }}
