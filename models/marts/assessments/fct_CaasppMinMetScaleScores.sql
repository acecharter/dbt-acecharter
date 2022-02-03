SELECT
  AceAssessmentId,
  Area,
  GradeLevel,
  CAST(Level3Min AS INT64) AS MinStandardMetScaleScore
FROM {{ ref('stg_GoogleSheetData__AssessmentScaleScoreRanges') }}