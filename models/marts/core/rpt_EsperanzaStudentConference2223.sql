with current_esperanza_students as (
    select * except (ExitWithdrawDate, ExitWithdrawReason)
    from {{ ref('dim_Students') }}
    where
        IsCurrentlyEnrolled = true
        and SchoolId = '129247'
),

attendance as (
    select *
    from {{ ref('fct_StudentAttendance') }}
    where SchoolYear = '2022-23'
),

assessments as (
    select *
    from {{ ref('fct_StudentAssessment') }}
    where AssessmentObjective = 'Overall'
),

star as (
    select
        * except (StudentResult),
        cast(StudentResult as float64) as StudentResult,
        case
            when date(AssessmentDate) < date('2022-12-01') then 'Fall'
            when
                date(AssessmentDate) >= date('2022-12-01')
                and date(AssessmentDate) < date('2023-04-01')
                then 'Winter'
            when date(AssessmentDate) < date('2023-07-01') then 'Spring'
        end as TestingWindow
    from assessments
    where
        starts_with(AssessmentName, 'Star')
        and ReportingMethod = 'Grade Equivalent (numeric)'
        and AssessmentSchoolYear = '2022-23'
),

star_reading as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarReadingGe
    from star
    where AssessmentSubject = 'Reading'
    group by 1, 2
),

star_reading_sp as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarReadingSpanishGe
    from star
    where AssessmentSubject = 'Reading (Spanish)'
    group by 1, 2
),

star_math as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarMathGe
    from star
    where AssessmentSubject = 'Math'
    group by 1, 2
),

star_math_sp as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarMathSpanishGe
    from star
    where AssessmentSubject = 'Math (Spanish)'
    group by 1, 2
),

star_el as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarEarlyLitGe
    from star
    where AssessmentSubject = 'Early Literacy'
    group by 1, 2
),

star_el_sp as (
    select
        StateUniqueId,
        TestingWindow,
        max(StudentResult) as StarEarlyLitSpanishGe
    from star
    where AssessmentSubject = 'Early Literacy (Spanish)'
    group by 1, 2
),

star_reading_fall as (
    select *
    from star_reading
    where TestingWindow = 'Fall'
),

star_reading_sp_fall as (
    select *
    from star_reading_sp
    where TestingWindow = 'Fall'
),

star_math_fall as (
    select *
    from star_math
    where TestingWindow = 'Fall'
),

star_math_sp_fall as (
    select *
    from star_math_sp
    where TestingWindow = 'Fall'
),

star_el_fall as (
    select *
    from star_el
    where TestingWindow = 'Fall'
),

star_el_sp_fall as (
    select *
    from star_el_sp
    where TestingWindow = 'Fall'
),

star_reading_winter as (
    select *
    from star_reading
    where TestingWindow = 'Winter'
),

star_reading_sp_winter as (
    select *
    from star_reading_sp
    where TestingWindow = 'Winter'
),

star_math_winter as (
    select *
    from star_math
    where TestingWindow = 'Winter'
),

star_math_sp_winter as (
    select *
    from star_math_sp
    where TestingWindow = 'Winter'
),

star_el_winter as (
    select *
    from star_el
    where TestingWindow = 'Winter'
),

star_el_sp_winter as (
    select *
    from star_el_sp
    where TestingWindow = 'Winter'
),

state_tests as (
    select * from assessments
    where
        AceAssessmentId in ('1', '2', '3', '4', '5', '6', '7', '8', '9')
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

grades_s1 as (
    select *
    from {{ ref ('fct_StudentGrades') }}
    where
        SessionName = 'SY2023 Semester 1'
        and GradeTypeDescriptor = 'Final'
),

grades_math_s1 as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as MathNumericGradeS1,
        LetterGradeEarned as MathLetterGradeS1
    from grades_s1
    where starts_with(SectionIdentifier, 'ACSMAT')
),

grades_ela_s1 as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as ElaNumericGradeS1,
        LetterGradeEarned as ElaLetterGradeS1
    from grades_s1
    where starts_with(SectionIdentifier, 'ACSENG')
),

grades_science_s1 as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as ScienceNumericGradeS1,
        LetterGradeEarned as ScienceLetterGradeS1
    from grades_s1
    where
        starts_with(SectionIdentifier, 'ACSLS')
        or starts_with(SectionIdentifier, 'ACSSCI')
        or starts_with(SectionIdentifier, 'ACSPS')
        or starts_with(SectionIdentifier, 'ACSESC')
),

grades_PE_s1 as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as PeNumericGradeS1,
        LetterGradeEarned as PeLetterGradeS1
    from grades_s1
    where starts_with(SectionIdentifier, 'ACSPED')
),

grades as (
    select *
    from {{ ref ('fct_StudentGrades') }}
    where
        IsCurrentGradingPeriod = true
        and GradingPeriodDescriptor = 'Second Semester'
),

grades_math as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as MathNumericGrade,
        LetterGradeEarned as MathLetterGrade
    from grades
    where starts_with(SectionIdentifier, 'ACSMAT')
),

grades_ela as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as ElaNumericGrade,
        LetterGradeEarned as ElaLetterGrade
    from grades
    where starts_with(SectionIdentifier, 'ACSENG')
),

grades_science as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as ScienceNumericGrade,
        LetterGradeEarned as ScienceLetterGrade
    from grades
    where
        starts_with(SectionIdentifier, 'ACSLS')
        or starts_with(SectionIdentifier, 'ACSSCI')
        or starts_with(SectionIdentifier, 'ACSPS')
        or starts_with(SectionIdentifier, 'ACSESC')
),

grades_PE as (
    select
        SchoolId,
        StudentUniqueId,
        NumericGradeEarned as PeNumericGrade,
        LetterGradeEarned as PeLetterGrade
    from grades
    where starts_with(SectionIdentifier, 'ACSPED')
),

other_data as (
    select *
    from {{ ref('stg_GSD__EsperanzaStudentConferenceData') }}
),

final as (
    select
        s.*,
        a.CountOfDaysAbsent,
        a.CountOfDaysEnrolled,
        round(a.AverageDailyAttendance, 2) as AttendanceRate,
        srf.StarReadingGe as StarReadingGeFall,
        srsf.StarReadingSpanishGe as StarReadingSpanishGeFall,
        smf.StarMathGe as StarMathGeFall,
        smsf.StarMathSpanishGe as StarMathSpanishGeFall,
        sef.StarEarlyLitGe as StarEarlyLitGeFall,
        sesf.StarEarlyLitSpanishGe as StarEarlyLitSpanishGFalle,
        srw.StarReadingGe as StarReadingGeWinter,
        srsw.StarReadingSpanishGe as StarReadingSpanishGeWinter,
        smw.StarMathGe as StarMathGeWinter,
        smsw.StarMathSpanishGe as StarMathSpanishGeWinter,
        sew.StarEarlyLitGe as StarEarlyLitGeWinter,
        sesw.StarEarlyLitSpanishGe as StarEarlyLitSpanishGeWinter,
        ge.ElaNumericGrade,
        ge.ElaLetterGrade,
        gm.MathNumericGrade,
        gm.MathLetterGrade,
        gs.ScienceNumericGrade,
        gs.ScienceLetterGrade,
        gp.PeNumericGrade,
        gp.PeLetterGrade,
        ges1.ElaNumericGradeS1,
        ges1.ElaLetterGradeS1,
        gms1.MathNumericGradeS1,
        gms1.MathLetterGradeS1,
        gss1.ScienceNumericGradeS1,
        gss1.ScienceLetterGradeS1,
        gps1.PeNumericGradeS1,
        gps1.PeLetterGradeS1,
        sbac_ela.SbacElaLevel,
        sbac_math.SbacMathLevel,
        ca_science.CastLevel,
        elpac.ElpacLevel,
        caa_ela.CaaElaLevel,
        caa_math.CaaMathLevel,
        caa_science.CaaScienceLevel,
        o.* except (StudentUniqueId)
    from current_esperanza_students as s
    left join attendance as a
        on
            s.SchoolId = a.SchoolId
            and s.StudentUniqueId = a.StudentUniqueId
    left join star_reading_fall as srf
        on s.StateUniqueId = srf.StateUniqueId
    left join star_reading_sp_fall as srsf
        on s.StateUniqueId = srsf.StateUniqueId
    left join star_math_fall as smf
        on s.StateUniqueId = smf.StateUniqueId
    left join star_math_sp_fall as smsf
        on s.StateUniqueId = smsf.StateUniqueId
    left join star_el_fall as sef
        on s.StateUniqueId = sef.StateUniqueId
    left join star_el_sp_fall as sesf
        on s.StateUniqueId = sesf.StateUniqueId
    left join star_reading_winter as srw
        on s.StateUniqueId = srw.StateUniqueId
    left join star_reading_sp_winter as srsw
        on s.StateUniqueId = srsw.StateUniqueId
    left join star_math_winter as smw
        on s.StateUniqueId = smw.StateUniqueId
    left join star_math_sp_winter as smsw
        on s.StateUniqueId = smsw.StateUniqueId
    left join star_el_winter as sew
        on s.StateUniqueId = sew.StateUniqueId
    left join star_el_sp_winter as sesw
        on s.StateUniqueId = sesw.StateUniqueId
    left join grades_ela as ge
        on
            s.SchoolId = ge.SchoolId
            and s.StudentUniqueId = ge.StudentUniqueId
    left join grades_math as gm
        on
            s.SchoolId = gm.SchoolId
            and s.StudentUniqueId = gm.StudentUniqueId
    left join grades_science as gs
        on
            s.SchoolId = gs.SchoolId
            and s.StudentUniqueId = gs.StudentUniqueId
    left join grades_pe as gp
        on
            s.SchoolId = gp.SchoolId
            and s.StudentUniqueId = gp.StudentUniqueId
    left join grades_ela_s1 as ges1
        on
            s.SchoolId = ges1.SchoolId
            and s.StudentUniqueId = ges1.StudentUniqueId
    left join grades_math_s1 as gms1
        on
            s.SchoolId = gms1.SchoolId
            and s.StudentUniqueId = gms1.StudentUniqueId
    left join grades_science_s1 as gss1
        on
            s.SchoolId = gss1.SchoolId
            and s.StudentUniqueId = gss1.StudentUniqueId
    left join grades_pe_s1 as gps1
        on
            s.SchoolId = gps1.SchoolId
            and s.StudentUniqueId = gps1.StudentUniqueId
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
    left join other_data as o
        on s.StudentUniqueId = o.StudentUniqueId
)

select * from final order by StateUniqueId
