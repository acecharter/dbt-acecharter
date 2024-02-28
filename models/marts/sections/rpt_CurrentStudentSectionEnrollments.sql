with current_schools as (
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

current_section_enrollments as (
    select *
    from {{ ref('fct_StudentSectionEnrollments') }}
    where IsCurrentSectionEnrollment = true
),

final as (
    select
        schools.SchoolYear,
        schools.SchoolNameMid as SchoolName,
        section_enrollments.SchoolId,
        section_enrollments.SessionName,
        section_enrollments.SectionIdentifier,
        sections.CourseCode,
        sections.CourseTitle,
        section_enrollments.ClassPeriodName,
        sections.Room,
        teachers.StaffDisplayName,
        section_enrollments.StudentUniqueId,
        students.StateUniqueId,
        students.DisplayName,
        students.GradeLevel,
        students.RaceEthnicity,
        students.IsEll,
        students.EllStatus,
        students.HasFrl,
        students.FrlStatus,
        students.HasIep
    from current_section_enrollments as section_enrollments
    left join current_schools as schools
    on section_enrollments.SchoolId = schools.SchoolId
    left join current_students as students
    on section_enrollments.StudentUniqueId = students.StudentUniqueId
    left join sections
    on section_enrollments.SchoolId = sections.SchoolId
    and section_enrollments.SessionName = sections.SessionName
    and section_enrollments.SectionIdentifier = sections.SectionIdentifier
    left join teachers
    on section_enrollments.SchoolId = teachers.SchoolId
    and section_enrollments.SessionName = teachers.SessionName
    and section_enrollments.SectionIdentifier = teachers.SectionIdentifier



)

select * from final
