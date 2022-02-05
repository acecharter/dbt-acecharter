WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    SystemOrVendorAssessmentId AS RecordType,
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
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
  CASE
    WHEN IncludeIndicator = 'false' THEN 'N'
    WHEN IncludeIndicator = 'true' THEN 'Y'
    ELSE IncludeIndicator
  END AS IncludeIndicator,
  CASE
    WHEN OverallScaleScore='NS' THEN NULL 
    ELSE OverallScaleScore 
  END AS OverallScaleScore,
  CASE 
    WHEN OralLanguageScaleScore='NS' THEN NULL
    ELSE OralLanguageScaleScore 
  END AS OralLanguageScaleScore,
  CASE 
    WHEN WrittenLanguageScaleScore='NS' THEN NULL
    ELSE WrittenLanguageScaleScore
  END AS WrittenLanguageScaleScore,
  CASE
    WHEN OverallPL='NS' THEN NULL
    ELSE OverallPL 
  END AS OverallPL,
  CASE
    WHEN OralLanguagePL='NS' THEN NULL 
    ELSE OralLanguagePL 
  END AS OralLanguagePL,
  CASE
    WHEN WrittenLanguagePL='NS' THEN NULL
    ELSE WrittenLanguagePL
  END AS WrittenLanguagePL,
  CASE
    WHEN ListeningPL='NS' THEN NULL
    ELSE ListeningPL
  END AS ListeningPL,
  CASE
    WHEN SpeakingPL='NS' THEN NULL
    ELSE SpeakingPL
  END AS SpeakingPL,
  CASE
    WHEN ReadingPL='NS' THEN NULL
    ELSE ReadingPL
  END AS ReadingPL,
  CASE
    WHEN WritingPL='NS' THEN NULL
    ELSE WritingPL
  END AS WritingPL,
  CASE
    WHEN AttemptednessMinus1='NS' THEN NULL
    ELSE AttemptednessMinus1
  END AS AttemptednessMinus1,
  CASE
    WHEN GradeAssessedMinus1='NS' THEN NULL
    ELSE GradeAssessedMinus1
  END AS GradeAssessedMinus1,
  CASE
    WHEN OverallScaleScoreMinus1='NS' THEN NULL
    ELSE OverallScaleScoreMinus1
  END AS OverallScaleScoreMinus1,
  CASE
    WHEN OverallPLMinus1='NS' THEN NULL
    ELSE OverallPLMinus1
  END AS OverallPLMinus1,
  CASE
    WHEN AttemptednessMinus2='NS' THEN NULL
    ELSE AttemptednessMinus2
  END AS AttemptednessMinus2,
  CASE
    WHEN GradeAssessedMinus2='NS' THEN NULL
    ELSE GradeAssessedMinus2
  END AS GradeAssessedMinus2,
  CASE
    WHEN OverallScaleScoreMinus2='NS' THEN NULL
    ELSE OverallScaleScoreMinus2
  END AS OverallScaleScoreMinus2,
  CASE
    WHEN OverallPLMinus2='NS' THEN NULL
    ELSE OverallPLMinus2
  END AS OverallPLMinus2,

FROM final
