select
    concat(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', StudentGroupId, '-', GradeLevel, '-', AssessmentType) AS AssessmentId,
    * except(TotalTested, TotalTestedWithScores),
    case when TestYear = 2018 THEN TotalTested ELSE TotalTestedWithScores END AS TotalTestedWithScores
from {{ ref('stg_RD__Elpac') }}
where
GradeLevel >= 5
and StudentGroupId in (
    '160', --All English learners - (All ELs) (Same as All Students (code 1) for ELPAC files)
    '120', --ELs enrolled less than 12 months
    '142', --ELs enrolled 12 months or more
    '242', --EL - 1 year in program
    '243', --EL - 2 years in program
    '244', --EL - 3 years in program
    '245', --EL - 4 years in program
    '246', --EL - 5 years in program
    '247' --EL - 6+ years in program
)