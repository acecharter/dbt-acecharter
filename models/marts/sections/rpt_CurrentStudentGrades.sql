with grades as (
    select * from {{ ref('fct_StudentGrades') }}
),

current_schools as (
    select * from {{ ref('dim_CurrentSchools') }}
),

current_students as (
    select *
    from {{ ref('dim_Students') }}
    where IsCurrentlyEnrolled = true
),

sections as (
    select * from {{ ref('dim_Sections') }}
),

teachers as (
    select *
    from {{ ref('dim_SectionStaff') }}
    where
        StaffClassroomPosition = 'Teacher of Record'
        and IsCurrentStaffAssociation = true
),

enrollments_ranked as (
    select
        *,
        rank() over (
            partition by
                SchoolId,
                SessionName,
                SectionIdentifier,
                ClassPeriodName,
                StudentUniqueId
            order by
                SchoolId asc,
                SessionName asc,
                SectionIdentifier asc,
                ClassPeriodName asc,
                StudentUniqueId asc,
                EndDate desc
        ) as Rank
    from {{ ref('fct_StudentSectionEnrollments') }}
),

final as (
    select
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
        e.BeginDate as SectionEnrollmentBeginDate,
        e.EndDate as SectionEnrollmentEndDate,
        e.IsCurrentSectionEnrollment,
        g.SchoolYear,
        g.GradingPeriodDescriptor,
        g.GradingPeriod,
        g.GradeTypeDescriptor,
        g.IsCurrentGradingPeriod,
        g.NumericGradeEarned,
        g.LetterGradeEarned
    from sections as s
    left join teachers as t
        on
            s.SchoolId = t.SchoolId
            and s.SessionName = t.SessionName
            and s.SectionIdentifier = t.SectionIdentifier
            and s.ClassPeriodName = t.ClassPeriodName
    left join enrollments_ranked as e
        on
            s.SchoolId = e.SchoolId
            and s.SessionName = e.SessionName
            and s.SectionIdentifier = e.SectionIdentifier
            and s.ClassPeriodName = e.ClassPeriodName
    left join grades as g
        on
            e.SchoolId = g.SchoolId
            and e.SessionName = g.SessionName
            and e.SectionIdentifier = g.SectionIdentifier
            and e.ClassPeriodName = g.ClassPeriodName
            and e.StudentUniqueId = g.StudentUniqueId
    left join current_schools as sc
        on e.SchoolId = sc.SchoolId
    left join current_students as st
        on
            e.SchoolId = st.SchoolId
            and e.StudentUniqueId = st.StudentUniqueId
    where
        e.Rank = 1
        and st.StudentUniqueId is not null
        and g.StudentUniqueId is not null
)

select * from final
