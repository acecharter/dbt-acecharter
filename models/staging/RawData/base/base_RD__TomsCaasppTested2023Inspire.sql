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
    CASE WHEN Section504Status = 'Yes' THEN TRUE ELSE FALSE END AS Section504Status,
    CASE WHEN CALPADSIDEAIndicator = 'Yes' THEN TRUE ELSE FALSE END AS CALPADSIDEAIndicator,
    CASE WHEN IDEAIndicatorForTesting = 'Yes' THEN TRUE ELSE FALSE END AS IDEAIndicatorForTesting,
    CASE WHEN MigrantStatus = 'Yes' THEN TRUE ELSE FALSE END AS MigrantStatus,
    CASE WHEN ELStatus = 'Yes' THEN TRUE ELSE FALSE END AS ELStatus,
    DATE(ELEntryDate) AS ELEntryDate,
    DATE(RFEPDate) AS RFEPDate,
    DATE(FirstEntryDateInUSSchool) AS FirstEntryDateInUSSchool,
    DATE(EnrollmentEffectiveDate) AS EnrollmentEffectiveDate,
    ELAS,
    CEDSLanguageCode,
    CALPADSPrimaryLanguage,
    CASE WHEN MilitaryStatus = 'Yes' THEN TRUE ELSE FALSE END AS MilitaryStatus,
    CASE WHEN FosterStatus = 'Yes' THEN TRUE ELSE FALSE END AS FosterStatus,
    CASE WHEN EconomicDisadvantageStatus = 'Yes' THEN TRUE ELSE FALSE END AS EconomicDisadvantageStatus,
    CASE WHEN EconomicDisadvantageTesting = 'Yes' THEN TRUE ELSE FALSE END AS EconomicDisadvantageTesting,
    CASE WHEN CALPADSNPSSchoolFlag = 'Y' THEN TRUE ELSE FALSE END AS CALPADSNPSSchoolFlag,
    CASE WHEN HispanicorLatino = 'Yes' THEN TRUE ELSE FALSE END AS HispanicorLatino,
    CASE WHEN AmericanIndianorAlaskaNative = 'Yes' THEN TRUE ELSE FALSE END AS AmericanIndianorAlaskaNative,
    CASE WHEN Asian = 'Yes' THEN TRUE ELSE FALSE END AS Asian,
    CASE WHEN HawaiianOrOtherPacificIslander = 'Yes' THEN TRUE ELSE FALSE END AS HawaiianOrOtherPacificIslander,
    CASE WHEN Filipino = 'Yes' THEN TRUE ELSE FALSE END AS Filipino,
    CASE WHEN BlackorAfricanAmerican = 'Yes' THEN TRUE ELSE FALSE END AS BlackorAfricanAmerican,
    CASE WHEN White = 'Yes' THEN TRUE ELSE FALSE END AS White,
    CASE WHEN TwoorMoreRaces = 'Yes' THEN TRUE ELSE FALSE END AS TwoorMoreRaces,
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
FROM {{ source('RawData', 'TomsCaasppTested2023Inspire')}}