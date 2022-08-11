SELECT
  CAST(RecordType AS STRING) AS RecordType,
  CAST(SSID AS STRING) AS SSID,
  StudentLastName,
  StudentFirstName,
  StudentMiddleName,
  DateofBirth,
  Gender,
  CAST(GradeAssessed AS STRING) AS GradeAssessed,
  CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS CALPADSSchoolCode,
  CALPADSSchoolName,
  Section504Status,
  CALPADSIDEAIndicator,
  IDEAIndicatorForTesting,
  MigrantStatus,
  ELStatus,
  ELEntryDate,
  FirstEntryDateInUSSchool,
  ELASforTesting,
  CEDSLanguageCode,
  CAST(PrimaryLanguageforTesting AS STRING) AS PrimaryLanguageforTesting,
  MilitaryStatus,
  FosterStatus,
  HomelessStatus,
  EconomicDisadvantageStatus,
  EconomicDisadvantageTesting,
  CAST(ReportingEthnicity AS STRING) AS ReportingEthnicity,
  CAST(ParentEducationLevel AS STRING) AS ParentEducationLevel,
  TestedStatus,
  CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS FinalTestedSchoolCode,
  DATE(FinalTestCompletedDate) AS FinalTestedCompletedDate,
  StudentExitCode,
  StudentExitWithdrawalDate,
  StudentRemovedCALPADSFileDate,
  ConditionCode,
  Attemptedness,
  CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
  CAST(REPLACE(CAST(OverallScaleScore AS STRING),'NS',NULL) AS INT64) AS OverallScaleScore,
  CAST(REPLACE(CAST(OverallPL AS STRING),'NS',NULL)AS INT64) AS OverallPL,
  CAST(REPLACE(CAST(OralLanguagePL AS STRING),'NS',NULL) AS INT64) AS OralLanguagePL,
  CAST(REPLACE(CAST(WrittenLanguagePL AS STRING),'NS',NULL) AS INT64) AS WrittenLanguagePL,
  CAST(REPLACE(CAST(ListeningPL AS STRING),'NS',NULL) AS INT64) AS ListeningPL,
  CAST(REPLACE(CAST(SpeakingPL AS STRING),'NS',NULL) AS INT64) AS SpeakingPL,
  CAST(REPLACE(CAST(ReadingPL AS STRING),'NS',NULL) AS INT64) AS WritingPL,
  CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
  CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
  CAST(REPLACE(CAST(OverallScaleScoreMinus1 AS STRING),'NS',NULL) AS INT64) AS OverallScaleScoreMinus1,
  CAST(REPLACE(CAST(OverallPLMinus1 AS STRING),'NS',NULL) AS INT64) AS OverallPLMinus1,
  CAST(REPLACE(CAST(OralLanguagePLMinus1 AS STRING),'NS',NULL) AS INT64) AS OralLanguagePLMinus1,
  CAST(REPLACE(CAST(WrittenLanguagePLMinus1 AS STRING),'NS',NULL) AS INT64) AS WrittenLanguagePLMinus1,
  CAST(REPLACE(CAST(ListeningPLMinus1 AS STRING),'NS',NULL) AS INT64) AS ListeningPLMinus1,
  CAST(REPLACE(CAST(SpeakingPLMinus1 AS STRING),'NS',NULL) AS INT64) AS SpeakingPLMinus1,
  CAST(REPLACE(CAST(ReadingPLMinus1 AS STRING),'NS',NULL) AS INT64) AS ReadingPLMinus1,
  CAST(REPLACE(CAST(WritingPLMinus1 AS STRING),'NS',NULL) AS INT64) AS WritingPLMinus1,
  CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
  CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
  CAST(REPLACE(CAST(OverallScaleScoreMinus2 AS STRING),'NS',NULL) AS INT64) AS OverallScaleScoreMinus2,
  CAST(REPLACE(CAST(OverallPLMinus2 AS STRING),'NS',NULL) AS INT64) AS OverallPLMinus2,
  CAST(REPLACE(CAST(OralLanguagePLMinus2 AS STRING),'NS',NULL) AS INT64) AS OralLanguagePLMinus2,
  CAST(REPLACE(CAST(WrittenLanguagePLMinus2 AS STRING),'NS',NULL) AS INT64) AS WrittenLanguagePLMinus2,
  CAST(REPLACE(CAST(ListeningPLMinus2 AS STRING),'NS',NULL) AS INT64) AS ListeningPLMinus2,
  CAST(REPLACE(CAST(SpeakingPLMinus2 AS STRING),'NS',NULL) AS INT64) AS SpeakingPLMinus2,
  CAST(REPLACE(CAST(ReadingPLMinus2 AS STRING),'NS',NULL) AS INT64) AS ReadingPLMinus2,
  CAST(REPLACE(CAST(WritingPLMinus2 AS STRING),'NS',NULL) AS INT64) AS WritingPLMinus2,
  CAST(AttemptednessMinus3 AS STRING) AS AttemptednessMinus3,
  CAST(GradeAssessedMinus3 AS STRING) AS GradeAssessedMinus3,
  CAST(REPLACE(CAST(OverallScaleScoreMinus3 AS STRING),'NS',NULL) AS INT64) AS OverallScaleScoreMinus3,
  CAST(REPLACE(CAST(OverallPLMinus3 AS STRING),'NS',NULL) AS INT64) AS OverallPLMinus3,
  CAST(REPLACE(CAST(OralLanguagePLMinus3 AS STRING),'NS',NULL) AS INT64) AS OralLanguagePLMinus3,
  CAST(REPLACE(CAST(WrittenLanguagePLMinus3 AS STRING),'NS',NULL) AS INT64) AS WrittenLanguagePLMinus3,
  CAST(REPLACE(CAST(ListeningPLMinus3 AS STRING),'NS',NULL) AS INT64) AS ListeningPLMinus3,
  CAST(REPLACE(CAST(SpeakingPLMinus3 AS STRING),'NS',NULL) AS INT64) AS SpeakingPLMinus3,
  CAST(REPLACE(CAST(ReadingPLMinus3 AS STRING),'NS',NULL) AS INT64) AS ReadingPLMinus3,
  CAST(REPLACE(CAST(WritingPLMinus3 AS STRING),'NS',NULL) AS INT64) AS WritingPLMinus3,
FROM {{ source('RawData', 'TomsElpacTested2022Empower')}}