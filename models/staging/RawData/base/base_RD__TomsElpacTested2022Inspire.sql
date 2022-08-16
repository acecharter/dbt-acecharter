SELECT
  CAST(RecordType AS STRING) AS RecordType,
  CAST(SSID AS STRING) AS SSID,
  StudentLastName,
  StudentFirstName,
  StudentMiddleName,
  DATE(DateofBirth) AS DateofBirth,
  Gender,
  CAST(GradeAssessed AS STRING) AS GradeAssessed,
  CAST(CAST(RIGHT(CAST(CALPADSSchoolCode AS STRING), 7) AS INT64) AS STRING) AS CALPADSSchoolCode,
  CALPADSSchoolName,
  Section504Status,
  CALPADSIDEAIndicator,
  IDEAIndicatorForTesting,
  PrimaryDisabilityType,
  PrimaryDisabilityforTesting,
  MigrantStatus,
  DATE(ELEntryDate) AS ELEntryDate,
  DATE(ELExitDate) AS ELExitDate,
  ELStatus,
  DATE(FirstEntryDateInUSSchool) AS FirstEntryDateInUSSchool,
  ELASforTesting,
  CALPADSPrimaryLanguage,
  CAST(PrimaryLanguageforTesting AS STRING) AS PrimaryLanguageforTesting,
  CEDSLanguageCode,
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
  DATE(StudentExitWithdrawalDate) AS StudentExitWithdrawalDate,
  DATE(StudentRemovedCALPADSFileDate) AS StudentRemovedCALPADSFileDate,
  ConditionCode,
  CAST(Attemptedness AS STRING) AS Attemptedness,
  CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
  CASE WHEN CAST(OverallScaleScore AS STRING) = 'NS' THEN NULL ELSE CAST(OverallScaleScore AS STRING) END AS OverallScaleScore,
  CAST(OverallPL AS STRING) AS OverallPL,
  CAST(OralLanguagePL AS STRING) AS OralLanguagePL,
  CAST(WrittenLanguagePL AS STRING) AS WrittenLanguagePL,
  CAST(ListeningPL AS STRING) AS ListeningPL,
  CAST(SpeakingPL AS STRING) AS SpeakingPL,
  CAST(ReadingPL AS STRING) AS WritingPL,
  CAST(AttemptednessMinus1 AS STRING) AS AttemptednessMinus1,
  CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
  CASE WHEN CAST(OverallScaleScoreMinus1 AS STRING) = 'NS' THEN NULL ELSE CAST(OverallScaleScoreMinus1 AS STRING) END AS OverallScaleScoreMinus1,
  CAST(OverallPLMinus1 AS STRING) AS OverallPLMinus1,
  CAST(OralLanguagePLMinus1 AS STRING) AS OralLanguagePLMinus1,
  CAST(WrittenLanguagePLMinus1 AS STRING) AS WrittenLanguagePLMinus1,
  CAST(ListeningPLMinus1 AS STRING) AS ListeningPLMinus1,
  CAST(SpeakingPLMinus1 AS STRING) AS SpeakingPLMinus1,
  CAST(ReadingPLMinus1 AS STRING) AS ReadingPLMinus1,
  CAST(WritingPLMinus1 AS STRING) AS WritingPLMinus1,
  CAST(AttemptednessMinus2 AS STRING) AS AttemptednessMinus2,
  CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
  CASE WHEN CAST(OverallScaleScoreMinus2 AS STRING) = 'NS' THEN NULL ELSE CAST(OverallScaleScoreMinus2 AS STRING) END AS OverallScaleScoreMinus2,
  CAST(OverallPLMinus2 AS STRING) AS OverallPLMinus2,
  CAST(OralLanguagePLMinus2 AS STRING) AS OralLanguagePLMinus2,
  CAST(WrittenLanguagePLMinus2 AS STRING) AS WrittenLanguagePLMinus2,
  CAST(ListeningPLMinus2 AS STRING) AS ListeningPLMinus2,
  CAST(SpeakingPLMinus2 AS STRING) AS SpeakingPLMinus2,
  CAST(ReadingPLMinus2 AS STRING) AS ReadingPLMinus2,
  CAST(WritingPLMinus2 AS STRING) AS WritingPLMinus2,
  CAST(AttemptednessMinus3 AS STRING) AS AttemptednessMinus3,
  CAST(GradeAssessedMinus3 AS STRING) AS GradeAssessedMinus3,
  CAST(OverallScaleScoreMinus3 AS STRING) AS OverallScaleScoreMinus3,
  CAST(OverallPLMinus3 AS STRING) AS OverallPLMinus3,
  CAST(OralLanguagePLMinus3 AS STRING) AS OralLanguagePLMinus3,
  CAST(WrittenLanguagePLMinus3 AS STRING) AS WrittenLanguagePLMinus3,
  CAST(ListeningPLMinus3 AS STRING) AS ListeningPLMinus3,
  CAST(SpeakingPLMinus3 AS STRING) AS SpeakingPLMinus3,
  CAST(ReadingPLMinus3 AS STRING) AS ReadingPLMinus3,
  CAST(WritingPLMinus3 AS STRING) AS WritingPLMinus3
FROM {{ source('RawData', 'TomsElpacTested2022Inspire')}}