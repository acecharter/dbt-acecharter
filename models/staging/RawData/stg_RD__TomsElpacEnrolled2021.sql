WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    SystemOrVendorAssessmentId AS RecordType,
  FROM {{ ref('stg_GSD__Assessments') }}
  WHERE AssessmentNameShort='Summative ELPAC'
),

empower AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(OverallScaleScore AS STRING) AS OverallScaleScore,
    CAST(OralLanguageScaleScore AS STRING) AS OralLanguageScaleScore,
    CAST(WrittenLanguageScaleScore AS STRING) AS WrittenLanguageScaleScore,
    CAST(OverallPL AS STRING) AS OverallPL,
    CAST(OralLanguagePL AS STRING) AS OralLanguagePL,
    CAST(WrittenLanguagePL AS STRING) AS WrittenLanguagePL,
    CAST(ListeningPL AS STRING) AS ListeningPL,
    CAST(SpeakingPL AS STRING) AS SpeakingPL,
    CAST(ReadingPL AS STRING) AS ReadingPL,
    CAST(WritingPL AS STRING) AS WritingPL,
    CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(OverallScaleScoreMinus1 AS STRING) AS OverallScaleScoreMinus1,
    CAST(OverallPLMinus1 AS STRING) AS OverallPLMinus1,
    CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(OverallScaleScoreMinus2 AS STRING) AS OverallScaleScoreMinus2,
    CAST(OverallPLMinus2 AS STRING) AS OverallPLMinus2
      FROM {{ source('RawData', 'TomsElpacEnrolled2021Empower')}}
),

esperanza AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(OverallScaleScore AS STRING) AS OverallScaleScore,
    CAST(OralLanguageScaleScore AS STRING) AS OralLanguageScaleScore,
    CAST(WrittenLanguageScaleScore AS STRING) AS WrittenLanguageScaleScore,
    CAST(OverallPL AS STRING) AS OverallPL,
    CAST(OralLanguagePL AS STRING) AS OralLanguagePL,
    CAST(WrittenLanguagePL AS STRING) AS WrittenLanguagePL,
    CAST(ListeningPL AS STRING) AS ListeningPL,
    CAST(SpeakingPL AS STRING) AS SpeakingPL,
    CAST(ReadingPL AS STRING) AS ReadingPL,
    CAST(WritingPL AS STRING) AS WritingPL,
    CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(OverallScaleScoreMinus1 AS STRING) AS OverallScaleScoreMinus1,
    CAST(OverallPLMinus1 AS STRING) AS OverallPLMinus1,
    CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(OverallScaleScoreMinus2 AS STRING) AS OverallScaleScoreMinus2,
    CAST(OverallPLMinus2 AS STRING) AS OverallPLMinus2
  FROM {{ source('RawData', 'TomsElpacEnrolled2021Esperanza')}}
),

inspire AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(OverallScaleScore AS STRING) AS OverallScaleScore,
    CAST(OralLanguageScaleScore AS STRING) AS OralLanguageScaleScore,
    CAST(WrittenLanguageScaleScore AS STRING) AS WrittenLanguageScaleScore,
    CAST(OverallPL AS STRING) AS OverallPL,
    CAST(OralLanguagePL AS STRING) AS OralLanguagePL,
    CAST(WrittenLanguagePL AS STRING) AS WrittenLanguagePL,
    CAST(ListeningPL AS STRING) AS ListeningPL,
    CAST(SpeakingPL AS STRING) AS SpeakingPL,
    CAST(ReadingPL AS STRING) AS ReadingPL,
    CAST(WritingPL AS STRING) AS WritingPL,
    CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(OverallScaleScoreMinus1 AS STRING) AS OverallScaleScoreMinus1,
    CAST(OverallPLMinus1 AS STRING) AS OverallPLMinus1,
    CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(OverallScaleScoreMinus2 AS STRING) AS OverallScaleScoreMinus2,
    CAST(OverallPLMinus2 AS STRING) AS OverallPLMinus2
  FROM {{ source('RawData', 'TomsElpacEnrolled2021Inspire')}}
),

hs AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    CAST(GradeAssessed AS STRING) AS GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    CAST(OverallScaleScore AS STRING) AS OverallScaleScore,
    CAST(OralLanguageScaleScore AS STRING) AS OralLanguageScaleScore,
    CAST(WrittenLanguageScaleScore AS STRING) AS WrittenLanguageScaleScore,
    CAST(OverallPL AS STRING) AS OverallPL,
    CAST(OralLanguagePL AS STRING) AS OralLanguagePL,
    CAST(WrittenLanguagePL AS STRING) AS WrittenLanguagePL,
    CAST(ListeningPL AS STRING) AS ListeningPL,
    CAST(SpeakingPL AS STRING) AS SpeakingPL,
    CAST(ReadingPL AS STRING) AS ReadingPL,
    CAST(WritingPL AS STRING) AS WritingPL,
    CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
    CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
    CAST(OverallScaleScoreMinus1 AS STRING) AS OverallScaleScoreMinus1,
    CAST(OverallPLMinus1 AS STRING) AS OverallPLMinus1,
    CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
    CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
    CAST(OverallScaleScoreMinus2 AS STRING) AS OverallScaleScoreMinus2,
    CAST(OverallPLMinus2 AS STRING) AS OverallPLMinus2
  FROM {{ source('RawData', 'TomsElpacEnrolled2021HighSchool')}}
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
    u.EnrolledSchoolId,
    u.TestedSchoolId,
    u.RecordType,
    u.SSID AS StateUniqueId,
    u.GradeAssessed,
    CASE
      WHEN u.Attemptedness = 'false' THEN 'N'
      WHEN u.Attemptedness = 'true' THEN 'Y'
      ELSE u.Attemptedness
    END AS Attemptedness,
    CASE
      WHEN u.IncludeIndicator = 'false' THEN 'N'
      WHEN u.IncludeIndicator = 'true' THEN 'Y'
      ELSE u.IncludeIndicator
    END AS IncludeIndicator,
    CASE
      WHEN u.OverallScaleScore='NS' THEN NULL 
      ELSE u.OverallScaleScore 
    END AS OverallScaleScore,
    CASE 
      WHEN u.OralLanguageScaleScore='NS' THEN NULL
      ELSE u.OralLanguageScaleScore 
    END AS OralLanguageScaleScore,
    CASE 
      WHEN u.WrittenLanguageScaleScore='NS' THEN NULL
      ELSE u.WrittenLanguageScaleScore
    END AS WrittenLanguageScaleScore,
    CASE
      WHEN u.OverallPL='NS' THEN NULL
      ELSE u.OverallPL 
    END AS OverallPL,
    CASE
      WHEN u.OralLanguagePL='NS' THEN NULL 
      ELSE u.OralLanguagePL 
    END AS OralLanguagePL,
    CASE
      WHEN u.WrittenLanguagePL='NS' THEN NULL
      ELSE u.WrittenLanguagePL
    END AS WrittenLanguagePL,
    CASE
      WHEN u.ListeningPL='NS' THEN NULL
      ELSE u.ListeningPL
    END AS ListeningPL,
    CASE
      WHEN u.SpeakingPL='NS' THEN NULL
      ELSE u.SpeakingPL
    END AS SpeakingPL,
    CASE
      WHEN u.ReadingPL='NS' THEN NULL
      ELSE u.ReadingPL
    END AS ReadingPL,
    CASE
      WHEN u.WritingPL='NS' THEN NULL
      ELSE u.WritingPL
    END AS WritingPL,
    CASE
      WHEN u.AttemptednessMinus1='NS' THEN NULL
      ELSE u.AttemptednessMinus1
    END AS AttemptednessMinus1,
    CASE
      WHEN u.GradeAssessedMinus1='NS' THEN NULL
      ELSE u.GradeAssessedMinus1
    END AS GradeAssessedMinus1,
    CASE
      WHEN u.OverallScaleScoreMinus1='NS' THEN NULL
      ELSE u.OverallScaleScoreMinus1
    END AS OverallScaleScoreMinus1,
    CASE
      WHEN u.OverallPLMinus1='NS' THEN NULL
      ELSE u.OverallPLMinus1
    END AS OverallPLMinus1,
    CASE
      WHEN u.AttemptednessMinus2='NS' THEN NULL
      ELSE u.AttemptednessMinus2
    END AS AttemptednessMinus2,
    CASE
      WHEN u.GradeAssessedMinus2='NS' THEN NULL
      ELSE u.GradeAssessedMinus2
    END AS GradeAssessedMinus2,
    CASE
      WHEN u.OverallScaleScoreMinus2='NS' THEN NULL
      ELSE u.OverallScaleScoreMinus2
    END AS OverallScaleScoreMinus2,
    CASE
      WHEN u.OverallPLMinus2='NS' THEN NULL
      ELSE u.OverallPLMinus2
    END AS OverallPLMinus2
  FROM unioned AS u
  LEFT JOIN assessment_ids AS a  
  USING (RecordType)
),

elpi_levels AS (
  SELECT *
  FROM {{ ref('stg_GSD__ElpiLevels') }}
),


final AS(
  SELECT
    r.*,
    e.ElpiLevel,
    e1.ElpiLevel AS ElpiLevelMinus1,
    e2.ElpiLevel AS ElpiLevelMinus2
  FROM results_with_assessment_id AS r
  LEFT JOIN elpi_levels AS e
  ON 
    r.GradeAssessed = CAST(e.GradeLevel AS STRING) AND
    CAST(r.OverallScaleScore AS INT64) BETWEEN CAST(e.MinScaleScore AS INT64) AND CAST(e.MaxScaleScore AS INT64)
  LEFT JOIN elpi_levels AS e1
  ON 
    r.GradeAssessedMinus1 = CAST(e1.GradeLevel AS STRING) AND
    CAST(r.OverallScaleScoreMinus1 AS INT64) BETWEEN CAST(e1.MinScaleScore AS INT64) AND CAST(e1.MaxScaleScore AS INT64)
  LEFT JOIN elpi_levels AS e2
  ON 
    r.GradeAssessedMinus2 = CAST(e2.GradeLevel AS STRING) AND
    CAST(r.OverallScaleScoreMinus2 AS INT64) BETWEEN CAST(e2.MinScaleScore AS INT64) AND CAST(e2.MaxScaleScore AS INT64)
)

SELECT
  AceAssessmentId,
  AssessmentName,
  RecordType,
  EnrolledSchoolId,
  TestedSchoolId,
  StateUniqueId,
  GradeAssessed,
  Attemptedness,
  IncludeIndicator,
  OverallScaleScore,
  OralLanguageScaleScore,
  WrittenLanguageScaleScore,
  OverallPL,
  ElpiLevel,
  OralLanguagePL,
  WrittenLanguagePL,
  ListeningPL,
  SpeakingPL,
  ReadingPL,
  WritingPL,
  AttemptednessMinus1,
  GradeAssessedMinus1,
  OverallScaleScoreMinus1,
  OverallPLMinus1,
  ElpiLevelMinus1,
  AttemptednessMinus2,
  GradeAssessedMinus2,
  OverallScaleScoreMinus2,
  OverallPLMinus2,
  ElpiLevelMinus2

FROM final
