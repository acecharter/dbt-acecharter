SELECT
  CAST(RecordType AS STRING) AS RecordType,
  CAST(SSID AS STRING) AS SSID,
  StudentLastName,
  StudentFirstName
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
  RFEPDate,
  FirstEntryDateInUSSchool,
  EnrollmentEffectiveDate,
  ELAS,
  CEDSLanguageCode,
  CAST(CALPADSPrimaryLanguage AS STRING) AS CALPADSPrimaryLanguage,
  MilitaryStatus,
  FosterStatus,
  HispanicorLatino,
  AmericanIndianorAlaskaNative,
  Asian,
  HawaiianOrOtherPacificIslander,
  Filipino,
  BlackorAfricanAmerican,
  White,
  TwoorMoreRaces,
  CAST(ReportingEthnicity AS STRING) AS ReportingEthnicity,
  CAST(CAST(RIGHT(CAST(FinalTestedSchoolCode AS STRING), 7) AS INT64) AS STRING) AS FinalTestedSchoolCode,
  StudentExitCode,
  StudentExitWithdrawalDate,
  StudentRemovedCALPADSFileDate,
  Attemptedness,
  ScoreStatus,
  IncludeIndicator,
  CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
  CAST(GrowthScore AS STRING) AS GrowthScore,
  CAST(ScaleScore AS STRING) AS ScaleScore,
  CAST(AchievementLevels AS STRING) AS AchievementLevels,
  CAST(GradeAssessedMinus1 AS STRING) AS GradeAssessedMinus1,
  CAST(ScaleScoreMinus1 AS STRING) AS ScaleScoreMinus1,
  CAST(AchievementLevelMinus1 AS STRING) AS AchievementLevelMinus1,
  CAST(GradeAssessedMinus2 AS STRING) AS GradeAssessedMinus2,
  CAST(ScaleScoreMinus2 AS STRING) AS ScaleScoreMinus2,
  CAST(AchievementLevelMinus2 AS STRING) AS AchievementLevelMinus2,
  CAST(GradeAssessedMinus3 AS STRING) AS GradeAssessedMinus3,
  CAST(ScaleScoreMinus3 AS STRING) AS ScaleScoreMinus3,
  CAST(AchievementLevelMinus3 AS STRING) AS AchievementLevelMinus3
FROM {{ source('RawData', 'TomsCaasppTested2022Esperanza')}}