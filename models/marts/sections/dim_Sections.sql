WITH
  academic_subjects AS (
    SELECT
      CourseCode,
      AcademicSubject
    FROM {{ ref('stg_GSD__CourseSubjects')}}
  ),

  sections AS (
    SELECT
      SchoolId,
      SessionName,
      SectionIdentifier,
      CourseCode,
      CourseTitle,
      CourseGpaApplicability,
      ClassPeriodName,
      Room,
      AvailableCredits,
      MIN(StaffBeginDate) AS SectionBeginDate,
      MAX(StaffEndDate) AS SectionEndDate  
    FROM {{ ref('stg_SP__CourseEnrollments_v2') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
  ),

  final AS (
    SELECT DISTINCT
      s.SchoolId,
      s.SessionName,
      s.SectionIdentifier,
      s.CourseCode,
      a.AcademicSubject,
      s.CourseTitle,
      s.CourseGpaApplicability,
      s.ClassPeriodName,
      s.Room,
      s.AvailableCredits,
      s.SectionBeginDate,
      s.SectionEndDate  
    FROM sections AS s
    LEFT JOIN academic_subjects AS a
    USING (CourseCode)
    ORDER BY 1, 2, 3, 4, 8
  )

SELECT * FROM final



