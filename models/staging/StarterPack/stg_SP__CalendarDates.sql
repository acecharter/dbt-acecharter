WITH
    calendar_dates AS (
        SELECT * FROM {{ source('StarterPack', 'CalendarDates')}}
    ),

    sy AS (
        SELECT CONCAT(MIN(EXTRACT(YEAR FROM CalendarDate)), '-',MIN(EXTRACT(YEAR FROM CalendarDate)-1999)) AS SchoolYear
        FROM calendar_dates
    ),

    final AS (
        SELECT
            sy.SchoolYear,
            c.*
        FROM calendar_dates AS c
        CROSS JOIN sy
    )

SELECT * FROM final