SELECT *
FROM {{ ref('rpt_CourseGrades')}}
WHERE IsCurrentGradingPeriod = TRUE