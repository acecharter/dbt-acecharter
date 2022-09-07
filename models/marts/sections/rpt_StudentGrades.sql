WITH
  grades AS (
    SELECT * FROM {{ ref('fct_StudentGrades')}}
  ),

  schools AS (
    SELECT * FROM {{ref('dim_CurrentSchools')}}
  ),

  students AS (
    SELECT * FROM {{ ref('dim_CurrentStudents') }}
  ),
  
  sections AS (
    SELECT * FROM {{ ref('dim_Sections') }}  
  ),

  teachers AS (
    SELECT *
    FROM {{ ref('dim_SectionStaff') }} 
    WHERE 
      StaffClassroomPosition = 'Teacher of Record'
      AND IsCurrentStaffAssociation=TRUE
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

  final AS (
    SELECT
      s.SchoolId,
      sc.SchoolName,
      sc.SchoolNameMid,
      sc.SchoolNameShort,
      s.CourseCode,
      s.CourseTitle,
      s.CourseGpaApplicability,
      s.AcademicSubject,
      s.SessionName,
      s.SectionIdentifier,
      s.ClassPeriodName,
      s.AvailableCredits,
      s.SectionBeginDate,
      s.SectionEndDate,
      t.StaffUniqueId,
      t.StaffDisplayName,
      t.StaffClassroomPosition,
      e.StudentUniqueId,
      st.StateUniqueId,
      st.DisplayName,
      st.Gender,
      st.RaceEthnicity,
      st.IsEll,
      st.EllStatus,
      st.HasFrl,
      st.FrlStatus,
      st.HasIep,
      st.SeisEligibilityStatus,
      st.GradeLevel,
      st.Email,
      st.EntryDate,
      st.ExitWithdrawDate,
      st.ExitWithdrawReason,
      st.IsCurrentlyEnrolled,
      e.BeginDate AS SectionEnrollmentBeginDate,
      e.EndDate As SectionEnrollmentEndDate,
      e.IsCurrentSectionEnrollment,
      g.GradingPeriodDescriptor,
      g.GradingPeriod,
      g.GradeTypeDescriptor,
      g.IsCurrentGradingPeriod,
      g.NumericGradeEarned,
      g.LetterGradeEarned
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
    LEFT JOIN grades AS g
    ON
      e.SchoolId = g.SchoolId
      AND e.SessionName = g.SessionName
      AND e.SectionIdentifier = g.SectionIdentifier
      AND e.ClassPeriodName = g.ClassPeriodName
      AND e.StudentUniqueId = g.StudentUniqueId
    LEFT JOIN schools AS sc
    ON e.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      e.SchoolId = st.SchoolId
      AND e.StudentUniqueId = st.StudentUniqueId
    WHERE e.Rank = 1
  )

SELECT * FROM final