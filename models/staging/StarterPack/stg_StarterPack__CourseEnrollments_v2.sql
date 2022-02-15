/*Fields dropped due to lack of use:
    - AcademicSubject (all NULL),
    - CourseLevelCharacteristic (all NULL)
*/


SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId
  LastSurname,
  FirstName,
  SessionName,
  SectionIdentifier
  StaffUniqueId,
  StaffDisplayName,
  ClassPeriodName,
  Room,
  CourseCode,
  CourseTitle,
  AvailableCredits,
  CourseGpaApplicability,
  BeginDate,
  EndDate,
  StaffClassroomPosition,
  StaffBeginDate,
  StaffEndDate,
  IsCurrentStaffAssociation

FROM {{ source('StarterPack', 'CourseEnrollments_v2')}}