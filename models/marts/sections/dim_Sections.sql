with academic_subjects as (
    select
        CourseCode,
        AcademicSubject
    from {{ ref('stg_GSD__CourseSubjects') }}
),

sections as (
    select
        SchoolId,
        SessionName,
        SectionIdentifier,
        CourseCode,
        CourseTitle,
        CourseGpaApplicability,
        ClassPeriodName,
        Room,
        AvailableCredits,
        min(BeginDate) as SectionBeginDate,
        max(EndDate) as SectionEndDate,
        min(StaffBeginDate) as SectionStaffBeginDate,
        max(StaffEndDate) as SectionStaffEndDate
    from {{ ref('stg_SP__CourseEnrollments_v2') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

final as (
    select distinct
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
        s.SectionEndDate,
        s.SectionStaffBeginDate,
        s.SectionStaffEndDate
    from sections as s
    left join academic_subjects as a
        on s.CourseCode = a.CourseCode
order by 1, 2, 3, 4, 8
)

select * from final
