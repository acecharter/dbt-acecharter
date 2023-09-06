with sections as (
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

joined as (
    select
        s.*,
        t.* except (
            SchoolId,
            SessionName,
            SectionIdentifier,
            ClassPeriodName
        ),
        e.* except (
            SchoolId,
            SessionName,
            SectionIdentifier,
            ClassPeriodName,
            Rank
        )
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
    where e.Rank = 1
),

final as (
    select
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
        BeginDate as StudentSectionBeginDate,
        EndDate as StudentSectionEndDate,
        IsCurrentSectionEnrollment
    from joined
)

select * from final
