WITH
  sections AS (
    SELECT * FROM {{ ref('dim_Sections') }}  
  ),

  teachers AS (
    SELECT
      *
    FROM {{ ref('dim_SectionStaff') }} 
    WHERE
      StaffClassroomPosition = 'Teacher of Record'
      AND IsCurrentStaffAssociation = TRUE
  ),

  enrollments_ranked AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY 
          SchoolId,
          SessionName,
          SectionIdentifier,
          ClassPeriodName,
          StudentUniqueId
        ORDER BY
          SchoolId,
          SessionName,
          SectionIdentifier,
          ClassPeriodName,
          StudentUniqueId,
          EndDate DESC
      ) AS Rank
    FROM {{ ref('fct_StudentSectionEnrollments') }}  
  ),

  joined AS (
    SELECT
      s.*,
      t.* EXCEPT(
        SchoolId,
        SessionName,
        SectionIdentifier,
        ClassPeriodName),
      e.* EXCEPT(
        SchoolId,
        SessionName,
        SectionIdentifier,
        ClassPeriodName,
        Rank),
    FROM sections AS s
    LEFT JOIN teachers AS t
      ON
        s.SchoolId = t.SchoolId
        AND s.SessionName = t.SessionName
        AND s.SectionIdentifier = t.SectionIdentifier
        AND s.ClassPeriodName = t.ClassPeriodName
    LEFT JOIN enrollments_ranked AS e
      ON 
        s.SchoolId = e.SchoolId
        AND s.SessionName = e.SessionName
        AND s.SectionIdentifier = e.SectionIdentifier
        AND s.ClassPeriodName = e.ClassPeriodName
    WHERE e.Rank = 1
  ),

  final AS (
    SELECT
      SchoolId,
      AcademicSubject,
      CourseCode,
      CourseTitle,
      CourseGpaApplicability,
      SessionName,
      SectionIdentifier,
      ClassPeriodName,
      Room,
      AvailableCredits,
      SectionBeginDate,
      SectionEndDate,
      StaffUniqueId,
      StaffDisplayName,
      StaffClassroomPosition,
      SectionStaffBeginDate,
      SectionStaffEndDate,
      IsCurrentStaffAssociation,
      StudentUniqueId,
      BeginDate AS StudentSectionBeginDate,
      EndDate AS StudentSectionEndDate,
      IsCurrentSectionEnrollment
    FROM joined
  )

SELECT * FROM final