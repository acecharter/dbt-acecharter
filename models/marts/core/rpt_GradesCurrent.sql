SELECT * 
FROM {{ ref('stg_StarterPack__CourseGrades') }}
WHERE
  IsCurrentGradingPeriod = true
  AND IsCurrentCourseEnrollment = true