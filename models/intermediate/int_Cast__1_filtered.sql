select
    concat(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', DemographicId, '-', GradeLevel, '-', TestId) as AssessmentId,
    *
from {{ ref('stg_RD__Cast')}}
where
    GradeLevel >= 5
    and DemographicId in (
        '1',   --All Students
        '128', --Reported Disabilities
        '31',  --Economic disadvantaged
        '160', --EL (English learner)
        '78',  --Hispanic or Latino
        '204'  --Economically disadvantaged Hispanic or Latino
    )