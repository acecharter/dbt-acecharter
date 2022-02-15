/*Fields dropped due to lack of use:
    - AcademicSubject (all NULL),
    - PerformanceBaseConversionDescriptor (all NULL)
*/

SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId,
  LastSurname,
  FirstName,
  SectionIdentifier,
  StaffUniqueId,
  StaffDisplayName,
  StaffClassroomPosition,
  ClassPeriodName,
  CourseCode,
  CourseTitle,
  AvailableCredits,
  SessionName,
  GradingPeriodDescriptor,
  GradeTypeDescriptor,
  IsCurrentCourseEnrollment,
  IsCurrentGradingPeriod,
  NumericGradeEarned,
  LetterGradeEarned

FROM {{ source('StarterPack', 'CourseGrades')}}
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE()