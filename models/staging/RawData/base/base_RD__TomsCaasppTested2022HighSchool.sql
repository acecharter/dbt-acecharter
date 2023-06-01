SELECT
    CAST(RecordType AS STRING) AS RecordType,
    CAST(SSID AS STRING) AS SSID,
    StudentLastName,
    StudentFirstName,
    StudentMiddleName,
    DateofBirth,
    Gender,
    CAST(CAST(GradeAssessed AS INT64) AS STRING) AS GradeAssessed,
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
    IFNULL(CAST(FosterStatus AS BOOL), FALSE) AS FosterStatus,
    EconomicDisadvantageStatus,
    EconomicDisadvantageTesting,
    CALPADSNPSSchoolFlag,
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
    ConditionCode,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CASE
        WHEN CAST(IncludeIndicator AS STRING) = 'true' THEN 'Y'
        WHEN CAST(IncludeIndicator AS STRING) = 'false' THEN 'N'
        ELSE CAST(IncludeIndicator AS STRING)
    END AS IncludeIndicator,
    CAST(LexileorQuantileMeasure AS STRING) AS LexileorQuantileMeasure,
    CAST(GrowthScore AS STRING) AS GrowthScore,
    CAST(ScaleScore AS INT64) AS ScaleScore,
    CAST(AchievementLevels AS INT64) AS AchievementLevels,
    CAST(CAST(GradeAssessedMinus1 AS INT64) AS STRING) AS GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS INT64) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS INT64) AS AchievementLevelMinus1,
    CAST(CAST(GradeAssessedMinus2 AS INT64) AS STRING) AS GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS INT64) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS INT64) AS AchievementLevelMinus2,
    CAST(CAST(GradeAssessedMinus3 AS INT64) AS STRING) AS GradeAssessedMinus3,
    CAST(ScaleScoreMinus3 AS INT64) AS ScaleScoreMinus3,
    CAST(AchievementLevelMinus3 AS INT64) AS AchievementLevelMinus3
FROM {{ source('RawData', 'TomsCaasppTested2022HighSchool')}}