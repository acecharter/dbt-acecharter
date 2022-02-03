WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    CAST(SystemOrVendorAssessmentId AS INT64) AS RecordType,
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
  WHERE AssessmentNameShort='Summative ELPAC'
),

empower AS (
  SELECT
    CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS EnrolledSchoolId,
    CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS TestedSchoolId,
    RecordType,
    SSID,
    GradeAssessed,
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
    RecordType,
    SSID,
    GradeAssessed,
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
    RecordType,
    SSID,
    GradeAssessed,
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
    RecordType,
    SSID,
    GradeAssessed,
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

final AS (
  SELECT
    a.AceAssessmentId,
    a.AssessmentName,
    u.*
  FROM unioned AS u
  LEFT JOIN assessment_ids AS a  
  USING (RecordType)
)


SELECT
  AceAssessmentId,
  AssessmentName,
  CAST(RecordType AS STRING) AS RecordType,
  EnrolledSchoolId,
  TestedSchoolId,
  CAST(SSID AS STRING) AS StateUniqueId,
  GradeAssessed,
  CASE
    WHEN Attemptedness = 'false' THEN 'N'
    WHEN Attemptedness = 'true' THEN 'Y'
    ELSE Attemptedness
  END AS Attemptedness,
  CASE
    WHEN IncludeIndicator = 'false' THEN 'N'
    WHEN IncludeIndicator = 'true' THEN 'Y'
    ELSE IncludeIndicator
  END AS IncludeIndicator,
  OverallScaleScore,
  OralLanguageScaleScore,
  WrittenLanguageScaleScore,
  OverallPL,
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
  AttemptednessMinus2,
  GradeAssessedMinus2,
  OverallScaleScoreMinus2,
  OverallPLMinus2
FROM final
