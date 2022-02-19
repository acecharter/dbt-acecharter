SELECT
  SchoolId,
  SessionName,
  SectionIdentifier,
  CourseCode,
  ClassPeriodName,
  Room,
  AvailableCredits,
  MIN(StaffBeginDate) AS CourseSectionBeginDate,
  MAX(StaffEndDate) AS CourseSectionEndDate  
FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6