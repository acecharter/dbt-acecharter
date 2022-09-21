with
  students as (
    select * except (ExitWithdrawDate, ExitWithdrawReason)
    from {{ ref('dim_CurrentStudents')}}
    where SchoolId = '129247'
  ),

  assessments as (
    select *
    from {{ ref('fct_StudentAssessment')}}
    where AssessmentObjective = 'Overall'
  ),

  star as (
    select * from assessments
    where STARTS_WITH(AssessmentName, 'Star')
    and ReportingMethod = 'Grade Equivalent'
    and AssessmentSchoolYear = '2022-23'
  ),

  star_reading as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarReadingGe
    from star
    where AssessmentSubject = 'Reading'
    group by StateUniqueId
  ),

  star_reading_sp as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarReadingSpanishGe
    from star
    where AssessmentSubject = 'Reading (Spanish)'
    GROUP BY StateUniqueId
  ),

  star_math as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarMathGe
    from star
    where AssessmentSubject = 'Math'
    group by StateUniqueId
  ),

  star_math_sp as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarMathSpanishGe
    from star
    where AssessmentSubject = 'Math (Spanish)'
    group by StateUniqueId
  ),

  star_el as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarEarlyLitGe
    from star
    where AssessmentSubject = 'Early Literacy'
    GROUP BY StateUniqueId
  ),

  star_el_sp as (
    select
      StateUniqueId,
      MAX(StudentResult) as StarEarlyLitSpanishGe
    from star
    where AssessmentSubject = 'Early Literacy (Spanish)'
    GROUP BY StateUniqueId
  ),

  state_tests as (
    select * from assessments
    where AceAssessmentId IN ('1','2','3','4','5','6','7','8','9')
    and ReportingMethod = 'Achievement Level'
    and AssessmentSchoolYear = '2021-22'
  ),

  sbac_ela as (
    select
      StateUniqueId,
      StudentResult as SbacElaLevel
    from state_tests
    where AceAssessmentId = '1'
  ),

  sbac_math as (
    select
      StateUniqueId,
      StudentResult as SbacMathLevel
    from state_tests
    where AceAssessmentId = '2'
  ),

  ca_science as (
    select
      StateUniqueId,
      StudentResult as CastLevel
    from state_tests
    where AceAssessmentId = '6'
  ),

  elpac as (
    select
      StateUniqueId,
      StudentResult as ElpacLevel
    from state_tests
    where AceAssessmentId = '8'
  ),

  caa_ela as (
    select
      StateUniqueId,
      StudentResult as CaaElaLevel
    from state_tests
    where AceAssessmentId = '3'
  ),

  caa_math as (
    select
      StateUniqueId,
      StudentResult as CaaMathLevel
    from state_tests
    where AceAssessmentId = '4'
  ),

  caa_science as (
    select
      StateUniqueId,
      StudentResult as CaaScienceLevel
    from state_tests
    where AceAssessmentId = '5'
  ),

  grades as (
    select *
    from {{ref ('fct_StudentGrades')}}
    where IsCurrentGradingPeriod = true
    and GradingPeriodDescriptor = 'First Semester'
  ),

  grades_math as (
    select
      SchoolId,
      StudentUniqueId,
      NumericGradeEarned as MathNumericGrade,
      LetterGradeEarned as MathLetterGrade
    from grades
    where STARTS_WITH(SectionIdentifier, 'ACSMAT')
  ),

  grades_ela as (
    select
      SchoolId,
      StudentUniqueId,
      NumericGradeEarned as ElaNumericGrade,
      LetterGradeEarned as ElaLetterGrade
    from grades
    where STARTS_WITH(SectionIdentifier, 'ACSENG')
  ),

  grades_science as (
    select
      SchoolId,
      StudentUniqueId,
      NumericGradeEarned as ScienceNumericGrade,
      LetterGradeEarned as ScienceLetterGrade
    from grades
    where STARTS_WITH(SectionIdentifier, 'ACSLS')
    OR STARTS_WITH(SectionIdentifier, 'ACSSCI')
    OR STARTS_WITH(SectionIdentifier, 'ACSPS')
    OR STARTS_WITH(SectionIdentifier, 'ACSESC')
  ),

  grades_PE as (
    select
      SchoolId,
      StudentUniqueId,
      NumericGradeEarned as PeNumericGrade,
      LetterGradeEarned as PeLetterGrade
    from grades
    where STARTS_WITH(SectionIdentifier, 'ACSPED')
  ),

  attendance as (
    select *
    from {{ ref('fct_StudentAttendance')}}
    where SchoolYear = '2022-23'
  ),

  final as (
    select
      s.*,
      sr.StarReadingGe,
      srs.StarReadingSpanishGe,
      sm.StarMathGe,
      sms.StarMathSpanishGe,
      se.StarEarlyLitGe,
      ses.StarEarlyLitSpanishGe,
      ge.ElaNumericGrade,
      ge.ElaLetterGrade,
      gm.MathNumericGrade,
      gm.MathLetterGrade,
      gs.ScienceNumericGrade,
      gs.ScienceLetterGrade,
      gp.PeNumericGrade,
      gp.PeLetterGrade,
      sbac_ela.SbacElaLevel,
      sbac_math.SbacMathLevel,
      ca_science.CastLevel,
      elpac.ElpacLevel,
      caa_ela.CaaElaLevel,
      caa_math.CaaMathLevel,
      caa_science.CaaScienceLevel,
      a.CountOfDaysAbsent,
      a.CountOfDaysEnrolled,
      Round(a.AverageDailyAttendance, 2) as AttendanceRate
    from students as s
    left join star_reading as sr
    on s.StateUniqueId = sr.StateUniqueId
    left join star_reading_sp as srs
    on s.StateUniqueId = srs.StateUniqueId
    left join star_math as sm
    on s.StateUniqueId = sm.StateUniqueId
    left join star_math_sp as sms
    on s.StateUniqueId = sms.StateUniqueId
    left join star_el as se
    on s.StateUniqueId = se.StateUniqueId
    left join star_el_sp as ses
    on s.StateUniqueId = ses.StateUniqueId
    left join grades_ela as ge
    on s.SchoolId = ge.SchoolId
    and s.StudentUniqueId = ge.StudentUniqueId
    left join grades_math as gm
    on s.SchoolId = gm.SchoolId
    and s.StudentUniqueId = gm.StudentUniqueId
    left join grades_science as gs
    on s.SchoolId = gs.SchoolId
    and s.StudentUniqueId = gs.StudentUniqueId
    left join grades_pe as gp
    on s.SchoolId = gp.SchoolId
    and s.StudentUniqueId = gp.StudentUniqueId
    left join sbac_ela
    on s.StateUniqueId = sbac_ela.StateUniqueId
    left join sbac_math
    on s.StateUniqueId = sbac_math.StateUniqueId
    left join ca_science
    on s.StateUniqueId = ca_science.StateUniqueId
    left join elpac
    on s.StateUniqueId = elpac.StateUniqueId
    left join caa_ela
    on s.StateUniqueId = caa_ela.StateUniqueId
    left join caa_math
    on s.StateUniqueId = caa_math.StateUniqueId
    left join caa_science
    on s.StateUniqueId = caa_science.StateUniqueId
    left join attendance as a
    on s.SchoolId = a.SchoolId
    and s.StudentUniqueId = a.StudentUniqueId

  )

select * from final