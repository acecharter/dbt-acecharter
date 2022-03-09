SELECT
  SchoolId,
  SessionName,
  SectionIdentifier,
  ClassPeriodName,
  StudentUniqueId,
  BeginDate,
  EndDate,
  CASE
    WHEN CURRENT_DATE('America/Los_Angeles') BETWEEN BeginDate AND EndDate
      THEN DATE_DIFF(CURRENT_DATE('America/Los_Angeles'), BeginDate, DAY)
    ELSE DATE_DIFF(EndDate, BeginDate, DAY) 
  END AS CountDaysEnrolledInSection,
  CASE
    WHEN CURRENT_DATE('America/Los_Angeles') BETWEEN BeginDate AND EndDate THEN TRUE 
    ELSE FALSE 
  END AS IsCurrentSectionEnrollment
FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6