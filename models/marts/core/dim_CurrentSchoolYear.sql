SELECT SchoolYear
FROM {{ ref('dim_SchoolYears')}}
WHERE YearsPriorToCurrent = 0