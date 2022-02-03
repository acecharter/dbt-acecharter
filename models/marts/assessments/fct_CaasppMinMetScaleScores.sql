SELECT
  AceAssessmentId,
  Area,
  GradeLevel,
  Level3Min AS MinStandardMetScaleScore
FROM {{ ref('stg_GoogleSheetData__AssessmentScaleScoreRanges') }}