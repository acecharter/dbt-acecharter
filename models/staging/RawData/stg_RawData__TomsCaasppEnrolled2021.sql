WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    CAST(SystemOrVendorAssessmentId AS INT64) AS RecordType,
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
  WHERE SystemOrVendorName='CAASPP'
),

empower AS (
  SELECT
    CAST(CAST(LEFT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS SchoolId,
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Empower')}}
),

esperanza AS (
  SELECT
    CAST(CAST(LEFT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS SchoolId,
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Esperanza')}}
),

inspire AS (
  SELECT
    CAST(CAST(LEFT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS SchoolId,
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Inspire')}}
),

hs AS (
  SELECT
    CAST(CAST(LEFT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS SchoolId,
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021HighSchool')}}
),

unioned AS (
    SELECT * FROM empower
    UNION All
    SELECT * FROM esperanza
    UNION All
    SELECT * FROM inspire
    UNION All
    SELECT * FROM hs
), 

results_with_assessment_id AS (
  SELECT
    a.AceAssessmentId,
    a.AssessmentName,
    u.*
  FROM unioned AS u
  LEFT JOIN assessment_ids AS a  
  USING (RecordType)
),

min_met_scores AS (
  SELECT
    AceAssessmentId,
    GradeLevel,
    MinStandardMetScaleScore
  FROM {{ ref('fct_CaasppMinMetScaleScores') }}
  WHERE Area='Overall'
),

final AS(
  SELECT
    r.*,
    CASE
      WHEN r.ScaleScore IS NOT NULL THEN CAST(r.ScaleScore AS INT64) - m.MinStandardMetScaleScore
      ELSE NULL
    END AS Dfs,
    m.MinStandardMetScaleScore,
    CASE
      WHEN r.ScaleScoreMinus1 IS NOT NULL THEN CAST(r.ScaleScoreMinus1 AS INT64) - m1.MinStandardMetScaleScore
      ELSE NULL
    END AS DfsMinus1,
    m1.MinStandardMetScaleScore AS MinStandardMetScaleScoreMinus1,
    CASE
      WHEN r.ScaleScoreMinus2 IS NOT NULL THEN CAST(r.ScaleScoreMinus2 AS INT64) - m2.MinStandardMetScaleScore
      ELSE NULL
    END AS DfsMinus2,
    m2.MinStandardMetScaleScore AS MinStandardMetScaleScoreMinus2,
  FROM results_with_assessment_id AS r
  
  LEFT JOIN min_met_scores AS m
  ON
    r.AceAssessmentId = m.AceAssessmentId AND
    CAST(r.GradeAssessed AS STRING) = m.GradeLevel

  LEFT JOIN min_met_scores AS m1
  ON
    r.AceAssessmentId = m1.AceAssessmentId AND
    r.GradeAssessedMinus1 = m1.GradeLevel
    
  LEFT JOIN min_met_scores AS m2
  ON
    r.AceAssessmentId = m2.AceAssessmentId AND
    r.GradeAssessedMinus2 = m2.GradeLevel
)


SELECT
  AceAssessmentId,
  AssessmentName,
  CAST(RecordType AS STRING) AS RecordType,
  SchoolId,
  CAST(SSID AS STRING) AS StateUniqueId,
  GradeAssessed,
  CASE
    WHEN Attemptedness = 'false' THEN 'N'
    WHEN Attemptedness = 'true' THEN 'Y'
    ELSE Attemptedness
  END AS Attemptedness,
  ScoreStatus,
  CASE
    WHEN IncludeIndicator = 'false' THEN 'N'
    WHEN IncludeIndicator = 'true' THEN 'Y'
    ELSE IncludeIndicator
  END AS IncludeIndicator,
  LexileorQuantileMeasure,
  AchievementLevels AS AchievementLevel,
  ScaleScore,
  MinStandardMetScaleScore,
  Dfs AS DistanceFromStandard,
  CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
  CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
  CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
  MinStandardMetScaleScoreMinus1,
  CAST(DfsMinus1 AS STRING) AS DistanceFromStandardMinus1,
  CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
  CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2,
  CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
  MinStandardMetScaleScoreMinus2,
  CAST(DfsMinus2 AS STRING) AS DistanceFromStandardMinus2
FROM final
