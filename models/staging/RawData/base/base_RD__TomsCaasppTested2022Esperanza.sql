select
    cast(RecordType as string) as RecordType,
    cast(SSID as string) as SSID,
    StudentLastName,
    StudentFirstName,
    StudentMiddleName,
    DateofBirth,
    Gender,
    cast(cast(GradeAssessed as int64) as string) as GradeAssessed,
    right(cast(CALPADSSchoolCode as string), 7) as CALPADSSchoolCode,
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
    cast(CALPADSPrimaryLanguage as string) as CALPADSPrimaryLanguage,
    MilitaryStatus,
    coalesce(cast(FosterStatus as bool), false) as FosterStatus,
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
    cast(ReportingEthnicity as string) as ReportingEthnicity,
    cast(
        cast(
            right(cast(FinalTestedSchoolCode as string), 7) as int64
        ) as string
    ) as FinalTestedSchoolCode,
    StudentExitCode,
    StudentExitWithdrawalDate,
    StudentRemovedCALPADSFileDate,
    ConditionCode,
    cast(Attemptedness as string) as Attemptedness,
    ScoreStatus,
    case
        when cast(IncludeIndicator as string) = 'true' then 'Y'
        when cast(IncludeIndicator as string) = 'false' then 'N'
        else cast(IncludeIndicator as string)
    end as IncludeIndicator,
    cast(LexileorQuantileMeasure as string) as LexileorQuantileMeasure,
    cast(GrowthScore as string) as GrowthScore,
    cast(ScaleScore as int64) as ScaleScore,
    cast(AchievementLevels as int64) as AchievementLevels,
    cast(cast(GradeAssessedMinus1 as int64) as string) as GradeAssessedMinus1,
    cast(ScaleScoreMinus1 as int64) as ScaleScoreMinus1,
    cast(AchievementLevelMinus1 as int64) as AchievementLevelMinus1,
    cast(cast(GradeAssessedMinus2 as int64) as string) as GradeAssessedMinus2,
    cast(ScaleScoreMinus2 as int64) as ScaleScoreMinus2,
    cast(AchievementLevelMinus2 as int64) as AchievementLevelMinus2,
    cast(cast(GradeAssessedMinus3 as int64) as string) as GradeAssessedMinus3,
    cast(ScaleScoreMinus3 as int64) as ScaleScoreMinus3,
    cast(AchievementLevelMinus3 as int64) as AchievementLevelMinus3
from {{ source('RawData', 'TomsCaasppTested2022Esperanza') }}
