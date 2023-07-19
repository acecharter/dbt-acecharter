select
    SchoolYear,
    StarTestingWindow,
    TestedSchoolId as SchoolId,
    StudentIdentifier as StudentUniqueId,
    AceAssessmentId,
    AssessmentName,
    AssessmentType,
    count(*) as ResultCount
from {{ ref('stg_RenaissanceStar') }}
where StudentIdentifier is not null
group by 1, 2, 3, 4, 5, 6, 7
