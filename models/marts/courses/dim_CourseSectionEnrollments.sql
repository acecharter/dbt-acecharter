SELECT
  SchoolId,
  SessionName,
  SectionIdentifier,
  ClassPeriodName,
  StudentUniqueId,
  BeginDate,
  EndDate
FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6