WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    CAST(SystemOrVendorAssessmentId AS STRING) AS RecordType,
  FROM {{ ref('stg_GSD__Assessments') }}
  WHERE SystemOrVendorName='CAASPP'
),

empower AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(ScoreStatus AS STRING) AS ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
    CAST(ScaleScore AS STRING) AS ScaleScore,
    CAST(AchievementLevels AS STRING) AS AchievementLevel,
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
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(ScoreStatus AS STRING) AS ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
    CAST(ScaleScore AS STRING) AS ScaleScore,
    CAST(AchievementLevels AS STRING) AS AchievementLevel,
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
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(ScoreStatus AS STRING) AS ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
    CAST(ScaleScore AS STRING) AS ScaleScore,
    CAST(AchievementLevels AS STRING) AS AchievementLevel,
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
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(ScoreStatus AS STRING) AS ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
    CAST(ScaleScore AS STRING) AS ScaleScore,
    CAST(AchievementLevels AS STRING) AS AchievementLevel,
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
    CAST(
      CASE
        WHEN r.ScaleScore IS NOT NULL THEN CAST(r.ScaleScore AS INT64) - m.MinStandardMetScaleScore
        ELSE NULL
      END AS STRING
    ) AS DistanceFromStandard,
    CAST(m.MinStandardMetScaleScore AS STRING) AS MinStandardMetScaleScore,
    CAST(
      CASE
        WHEN r.ScaleScoreMinus1 IS NOT NULL THEN CAST(r.ScaleScoreMinus1 AS INT64) - m1.MinStandardMetScaleScore
        ELSE NULL
      END AS STRING
    ) AS DistanceFromStandardMinus1,
    CAST(m1.MinStandardMetScaleScore AS STRING) AS MinStandardMetScaleScoreMinus1,
    CAST(
      CASE
        WHEN r.ScaleScoreMinus2 IS NOT NULL THEN CAST(r.ScaleScoreMinus2 AS INT64) - m2.MinStandardMetScaleScore
        ELSE NULL
      END AS STRING
    ) AS DistanceFromStandardMinus2,
    CAST(m2.MinStandardMetScaleScore AS STRING) AS MinStandardMetScaleScoreMinus2,
  FROM results_with_assessment_id AS r
  
  LEFT JOIN min_met_scores AS m
  ON
    r.AceAssessmentId = m.AceAssessmentId AND
    r.GradeAssessed = m.GradeLevel

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
  RecordType,
  EnrolledSchoolId,
  TestedSchoolId,
  SSID AS StateUniqueId,
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
  CASE
    WHEN AchievementLevel='9' THEN NULL
    ELSE AchievementLevel
  END AS AchievementLevel,
  ScaleScore,
  MinStandardMetScaleScore,
  DistanceFromStandard,
  GradeAssessedMinus1,
  CASE
    WHEN AchievementLevelMinus1='9' THEN NULL
    ELSE AchievementLevelMinus1
  END AS AchievementLevelMinus1,
  ScaleScoreMinus1,
  MinStandardMetScaleScoreMinus1,
  DistanceFromStandardMinus1,
  GradeAssessedMinus2,
  CASE
    WHEN AchievementLevelMinus2='9' THEN NULL
    ELSE AchievementLevelMinus2
  END AS AchievementLevelMinus2,
  ScaleScoreMinus2,
  MinStandardMetScaleScoreMinus2,
  DistanceFromStandardMinus2
FROM final
