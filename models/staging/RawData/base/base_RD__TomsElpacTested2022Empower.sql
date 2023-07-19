select
    cast(RecordType as string) as RecordType,
    cast(SSID as string) as SSID,
    StudentLastName,
    StudentFirstName,
    StudentMiddleName,
    date(DateofBirth) as DateofBirth,
    Gender,
    cast(GradeAssessed as string) as GradeAssessed,
    cast(cast(right(cast(CALPADSSchoolCode as string), 7) as int64) as string)
        as CALPADSSchoolCode,
    CALPADSSchoolName,
    cast(Section504Status as bool) as Section504Status,
    cast(CALPADSIDEAIndicator as bool) as CALPADSIDEAIndicator,
    cast(IDEAIndicatorForTesting as bool) as IDEAIndicatorForTesting,
    PrimaryDisabilityType,
    PrimaryDisabilityforTesting,
    cast(MigrantStatus as bool) as MigrantStatus,
    date(ELEntryDate) as ELEntryDate,
    date(ELExitDate) as ELExitDate,
    cast(ELStatus as bool) as ELStatus,
    date(FirstEntryDateInUSSchool) as FirstEntryDateInUSSchool,
    ELASforTesting,
    CALPADSPrimaryLanguage,
    cast(PrimaryLanguageforTesting as string) as PrimaryLanguageforTesting,
    CEDSLanguageCode,
    cast(MilitaryStatus as bool) as MilitaryStatus,
    cast(FosterStatus as bool) as FosterStatus,
    cast(HomelessStatus as bool) as HomelessStatus,
    cast(EconomicDisadvantageStatus as bool) as EconomicDisadvantageStatus,
    cast(EconomicDisadvantageTesting as bool) as EconomicDisadvantageTesting,
    cast(ReportingEthnicity as string) as ReportingEthnicity,
    cast(ParentEducationLevel as string) as ParentEducationLevel,
    TestedStatus,
    cast(
        cast(right(cast(FinalTestedSchoolCode as string), 7) as int64) as string
    ) as FinalTestedSchoolCode,
    date(FinalTestCompletedDate) as FinalTestedCompletedDate,
    StudentExitCode,
    date(StudentExitWithdrawalDate) as StudentExitWithdrawalDate,
    date(StudentRemovedCALPADSFileDate) as StudentRemovedCALPADSFileDate,
    ConditionCode,
    cast(Attemptedness as string) as Attemptedness,
    cast(IncludeIndicator as string) as IncludeIndicator,
    case
        when cast(OverallScaleScore as string) = 'NS' then null else
            cast(OverallScaleScore as string)
    end as OverallScaleScore,
    cast(OverallPL as string) as OverallPL,
    cast(OralLanguagePL as string) as OralLanguagePL,
    cast(WrittenLanguagePL as string) as WrittenLanguagePL,
    cast(ListeningPL as string) as ListeningPL,
    cast(SpeakingPL as string) as SpeakingPL,
    cast(ReadingPL as string) as ReadingPL,
    cast(WritingPL as string) as WritingPL,
    cast(AttemptednessMinus1 as string) as AttemptednessMinus1,
    cast(GradeAssessedMinus1 as string) as GradeAssessedMinus1,
    case
        when cast(OverallScaleScoreMinus1 as string) = 'NS' then null else
            cast(OverallScaleScoreMinus1 as string)
    end as OverallScaleScoreMinus1,
    cast(OverallPLMinus1 as string) as OverallPLMinus1,
    cast(OralLanguagePLMinus1 as string) as OralLanguagePLMinus1,
    cast(WrittenLanguagePLMinus1 as string) as WrittenLanguagePLMinus1,
    cast(ListeningPLMinus1 as string) as ListeningPLMinus1,
    cast(SpeakingPLMinus1 as string) as SpeakingPLMinus1,
    cast(ReadingPLMinus1 as string) as ReadingPLMinus1,
    cast(WritingPLMinus1 as string) as WritingPLMinus1,
    cast(AttemptednessMinus2 as string) as AttemptednessMinus2,
    cast(GradeAssessedMinus2 as string) as GradeAssessedMinus2,
    case
        when cast(OverallScaleScoreMinus2 as string) = 'NS' then null else
            cast(OverallScaleScoreMinus2 as string)
    end as OverallScaleScoreMinus2,
    cast(OverallPLMinus2 as string) as OverallPLMinus2,
    cast(OralLanguagePLMinus2 as string) as OralLanguagePLMinus2,
    cast(WrittenLanguagePLMinus2 as string) as WrittenLanguagePLMinus2,
    cast(ListeningPLMinus2 as string) as ListeningPLMinus2,
    cast(SpeakingPLMinus2 as string) as SpeakingPLMinus2,
    cast(ReadingPLMinus2 as string) as ReadingPLMinus2,
    cast(WritingPLMinus2 as string) as WritingPLMinus2,
    cast(AttemptednessMinus3 as string) as AttemptednessMinus3,
    cast(GradeAssessedMinus3 as string) as GradeAssessedMinus3,
    cast(OverallScaleScoreMinus3 as string) as OverallScaleScoreMinus3,
    cast(OverallPLMinus3 as string) as OverallPLMinus3,
    cast(OralLanguagePLMinus3 as string) as OralLanguagePLMinus3,
    cast(WrittenLanguagePLMinus3 as string) as WrittenLanguagePLMinus3,
    cast(ListeningPLMinus3 as string) as ListeningPLMinus3,
    cast(SpeakingPLMinus3 as string) as SpeakingPLMinus3,
    cast(ReadingPLMinus3 as string) as ReadingPLMinus3,
    cast(WritingPLMinus3 as string) as WritingPLMinus3
from {{ source('RawData', 'TomsElpacTested2022Empower') }}
