select
    concat(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', DemographicId, '-', GradeLevel, '-', TestId) as AssessmentId,
    *
from {{ ref('stg_RD__Cast')}}
where
    GradeLevel >= 5
    and DemographicId in (
        '1',   --All Students
        '128', --Reported disabilities
        '99',  --No reported disabilities
        '31',  --Economic disadvantaged
        '111', --Not economically disadvantaged
        '160', --EL (English learner)
        '8',   --RFEP (Reclassified fluent English proficient)
        '6',   --IFEP and EO (Initial fluent English proficient and English only)
        '78',  --Hispanic or Latino
        '204'  --Economically disadvantaged Hispanic or Latino
    )