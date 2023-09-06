select
    SchoolId,
    SessionName,
    SectionIdentifier,
    ClassPeriodName,
    StaffUniqueId,
    StaffDisplayName,
    StaffClassroomPosition,
    StaffBeginDate,
    StaffEndDate,
    IsCurrentStaffAssociation
from {{ ref('stg_SP__CourseEnrollments_v2') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
order by 1, 2, 3, 4, 5, 6, 7, 8
