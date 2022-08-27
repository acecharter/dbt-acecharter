SELECT CONCAT(MIN(EXTRACT(YEAR FROM CalendarDate)), '-',MIN(EXTRACT(YEAR FROM CalendarDate)-1999)) AS SchoolYear
FROM {{ source('StarterPack', 'CalendarDates')}}