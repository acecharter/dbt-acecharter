SELECT
    RecordType,
    SSID,
    StudentLastName,
    StudentFirstName,
    StudentMiddleName,
    DATE(DateofBirth) AS DateofBirth,
    Gender,
    GradeAssessed,
    CAST(CAST(RIGHT(CALPADSSchoolCode, 7) AS INT64) AS STRING) AS CALPADSSchoolCode,
    CALPADSSchoolName,
    Section504Status,
    CALPADSIDEAIndicator,
    IDEAIndicatorForTesting,
    MigrantStatus,
    ELStatus,
    DATE(ELEntryDate) AS ELEntryDate,
    DATE(RFEPDate) AS RFEPDate,
    DATE(FirstEntryDateInUSSchool) AS FirstEntryDateInUSSchool,
    DATE(EnrollmentEffectiveDate) AS EnrollmentEffectiveDate,
    ELAS,
    CEDSLanguageCode,
    CALPADSPrimaryLanguage,
    MilitaryStatus,
    FosterStatus,
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
    ReportingEthnicity,
    CAST(CAST(RIGHT(FinalTestedSchoolCode, 7) AS INT64) AS STRING) AS FinalTestedSchoolCode,
    StudentExitCode,
    DATE(StudentExitWithdrawalDate) AS StudentExitWithdrawalDate,
    DATE(StudentRemovedCALPADSFileDate) AS StudentRemovedCALPADSFileDate,
    ConditionCode,
    Attemptedness,
    ScoreStatus,
    CASE
        WHEN IncludeIndicator = 'true' THEN 'Y'
        WHEN IncludeIndicator = 'false' THEN 'N'
        ELSE IncludeIndicator
    END AS IncludeIndicator,
    LexileorQuantileMeasure,
    GrowthScore,
    CAST(ScaleScore AS INT64) AS ScaleScore,
    CAST(AchievementLevels AS INT64) AS AchievementLevels,
    GradeAssessedMinus1,
    CAST(ScaleScoreMinus1 AS INT64) AS ScaleScoreMinus1,
    CAST(AchievementLevelMinus1 AS INT64) AS AchievementLevelMinus1,
    GradeAssessedMinus2,
    CAST(ScaleScoreMinus2 AS INT64) AS ScaleScoreMinus2,
    CAST(AchievementLevelMinus2 AS INT64) AS AchievementLevelMinus2,
    GradeAssessedMinus3,
    CAST(ScaleScoreMinus3 AS INT64) AS ScaleScoreMinus3,
    CAST(AchievementLevelMinus3 AS INT64) AS AchievementLevelMinus3
FROM {{ source('RawData', 'TomsCaasppTested2023HighSchool')}}