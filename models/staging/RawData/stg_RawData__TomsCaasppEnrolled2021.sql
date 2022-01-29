WITH assessment_ids AS (
  SELECT 
    AceAssessmentUniqueId,
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

final AS (
  SELECT
    a.AceAssessmentUniqueId,
    a.AssessmentName,
    u.*
  FROM unioned AS u
  LEFT JOIN assessment_ids AS a  
  USING (RecordType)
)

SELECT
  AceAssessmentUniqueId,
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
  ScaleScore,
  AchievementLevels,
  CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
  CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
  CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
  CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
  CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
  CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2
FROM final
