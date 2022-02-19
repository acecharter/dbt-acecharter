SELECT
  SchoolId,
  SessionName,
  SectionIdentifier,
  ClassPeriodName,
  StaffUniqueId,
  StaffClassroomPosition,
  StaffBeginDate,
  StaffEndDate,
  IsCurrentStaffAssociation
FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1, 2, 3, 4, 5, 6, 7