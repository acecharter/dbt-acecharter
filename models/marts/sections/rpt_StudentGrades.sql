WITH
  course_enrollments AS (
    SELECT * FROM {{ ref('dim_CourseEnrollments')}}
  ),

  course_grades AS (
    SELECT * FROM {{ ref('fct_StudentGrades')}}
  ),

  schools AS (
    SELECT * FROM {{ref('dim_Schools')}}
  ),

  students AS (
    SELECT * FROM {{ ref('dim_Students') }}
  ),

  final AS (
    SELECT
      e.SchoolId,
      sc.SchoolName,
      sc.SchoolNameMid,
      sc.SchoolNameShort,
      e.CourseCode,
      e.CourseTitle,
      e.CourseGpaApplicability,
      e.AcademicSubject,
      e.SessionName,
      e.SectionIdentifier,
      e.ClassPeriodName,
      e.AvailableCredits,
      e.SectionBeginDate,
      e.SectionEndDate,
      e.StaffUniqueId,
      e.StaffDisplayName,
      e.StaffClassroomPosition,
      e.StudentUniqueId,
      st.StateUniqueId,
      st.DisplayName AS StudentName,
      st.Gender,
      st.RaceEthnicity,
      st.IsEll,
      st.EllStatus,
      st.HasFrl,
      st.FrlStatus,
      st.HasIep,
      st.SeisEligibilityStatus,
      st.GradeLevel,
      st.EntryDate,
      st.ExitWithdrawDate,
      st.ExitWithdrawReason,
      st.IsCurrentlyEnrolled,
      e.BeginDate AS CourseEnrollmentBeginDate,
      e.EndDate As CourseEnrollmentEndDate,
      e.IsCurrentSectionEnrollment,
      g.GradingPeriodDescriptor,
      g.GradeTypeDescriptor,
      g.IsCurrentGradingPeriod,
      g.NumericGradeEarned,
      g.LetterGradeEarned
    FROM course_enrollments AS e
    LEFT JOIN course_grades AS g
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
  )

SELECT * FROM final