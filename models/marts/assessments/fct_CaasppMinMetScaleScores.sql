select
    AceAssessmentId,
    Area,
    GradeLevel,
    cast(Level3Min as int64) as MinStandardMetScaleScore
from {{ ref('stg_GSD__AssessmentScaleScoreRanges') }}
