with
    elpac as (
        select * from {{ ref('base_RD__TomsElpacEnrolledEmpower')}}
        union all select * from {{ ref('base_RD__TomsElpacEnrolledEsperanza')}}
        union all select * from {{ ref('base_RD__TomsElpacEnrolledInspire')}}
        union all select * from {{ ref('base_RD__TomsElpacEnrolledHighSchool')}}
    ),
    
assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId
    from {{ ref('stg_GSD__Assessments') }}
    where SystemOrVendorName = 'ELPAC'
),

elpac_yrs_id_added as (
    select
        cast(extract(year from FinalTestedCompletedDate) as int64) as TestYear,
        concat(cast(cast(extract(year from FinalTestedCompletedDate) - 1 as int64) as string),'-', cast(cast(extract(year from FinalTestedCompletedDate) - 2000 as int64) as string)) as SchoolYear,
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.AssessmentSubject,
        e.*
    from elpac as e
    left join assessment_ids as a
    on e.RecordType = a.SystemOrVendorAssessmentId
),

elpi_levels as (
    select * from {{ ref('stg_GSD__ElpiLevels') }}
),

elpac_elpi_level_added as (
    select
        r.*,
        e.ElpiLevel,
        e.ElpiLevelNumeric,
        e.ElpiLevelRank,
        e1.ElpiLevel as ElpiLevelMinus1,
        e1.ElpiLevelNumeric as ElpiLevelNumericMinus1,
        e1.ElpiLevelRank as ElpiLevelRankMinus1,
        e2.ElpiLevel as ElpiLevelMinus2,
        e2.ElpiLevelNumeric as ElpiLevelNumericMinus2,
        e2.ElpiLevelRank as ElpiLevelRankMinus2,
        e3.ElpiLevel as ElpiLevelMinus3,
        e3.ElpiLevelNumeric as ElpiLevelNumericMinus3,
        e3.ElpiLevelRank as ElpiLevelRankMinus3,
    from elpac_yrs_id_added as r
    left join elpi_levels as e
    on 
        r.GradeAssessed = cast(e.GradeLevel as string) and
        cast(r.OverallScaleScore as int64) between cast(e.MinScaleScore as int64) and cast(e.MaxScaleScore as int64)
    left join elpi_levels as e1
    on 
        r.GradeAssessedMinus1 = cast(e1.GradeLevel as string) and
        cast(r.OverallScaleScoreMinus1 as int64) between cast(e1.MinScaleScore as int64) and cast(e1.MaxScaleScore as int64)
    left join elpi_levels as e2
    on 
        r.GradeAssessedMinus2 = cast(e2.GradeLevel as string) and
        cast(r.OverallScaleScoreMinus2 as int64) between cast(e2.MinScaleScore as int64) and cast(e2.MaxScaleScore as int64)
    left join elpi_levels as e3
    on 
        r.GradeAssessedMinus3 = cast(e3.GradeLevel as string) and
        cast(r.OverallScaleScoreMinus3 as int64) between cast(e3.MinScaleScore as int64) and cast(e3.MaxScaleScore as int64)
),

elpi_change_added as (
    select
        *,
        ElpiLevelRank - ElpiLevelRankMinus1 as ElpiLevelChange,
        case
            when ElpiLevelNumeric = ElpiLevelNumericMinus1 then concat('Maintained at ', ElpiLevel)
            when ElpiLevelNumeric > ElpiLevelNumericMinus1 then 'Increased'
            when ElpiLevelNumeric < ElpiLevelNumericMinus1 then 'Declined'
            else 'ERROR'
        end as ElpiChangeCategory,
        (ElpiLevelNumeric = 4 and ElpiLevelNumericMinus1 = 4) or ElpiLevelNumeric - ElpiLevelNumericMinus1 > 0 as ElpiProgress,
        ElpiLevelRankMinus1 - ElpiLevelRankMinus2 as ElpiLevelChangeMinus1,
        case
            when ElpiLevelNumericMinus1 = ElpiLevelNumericMinus2 then concat('Maintained at ', ElpiLevelMinus1)
            when ElpiLevelNumericMinus1 > ElpiLevelNumericMinus2 then 'Increased'
            when ElpiLevelNumericMinus1 < ElpiLevelNumericMinus2 then 'Declined'
            else 'ERROR'
        end as ElpiChangeCategoryMinus1,
        (ElpiLevelNumericMinus1 = 4 and ElpiLevelNumericMinus2 = 4) or ElpiLevelNumericMinus1 - ElpiLevelNumericMinus2 > 0 as ElpiProgressMinus1,
        ElpiLevelRankMinus2 - ElpiLevelRankMinus3 as ElpiLevelChangeMinus2,
        case
            when ElpiLevelNumericMinus2 = ElpiLevelNumericMinus3 then concat('Maintained at ', ElpiLevelMinus2)
            when ElpiLevelNumericMinus2 > ElpiLevelNumericMinus3 then 'Increased'
            when ElpiLevelNumericMinus2 < ElpiLevelNumericMinus3 then 'Declined'
            else 'ERROR'
        end as ElpiChangeCategoryMinus2,
        (ElpiLevelNumericMinus2 = 4 and ElpiLevelNumericMinus3 = 4) or ElpiLevelNumericMinus2 - ElpiLevelNumericMinus3 > 0 as ElpiProgressMinus2,
    from elpac_elpi_level_added
),

final as (
    select
        AceAssessmentId,
        AceAssessmentName,
        AssessmentSubject,
        RecordType,
        TestYear,
        SchoolYear,
        SSID,
        StudentLastName,
        StudentFirstName,
        StudentMiddleName,
        DateofBirth,
        Gender,
        GradeAssessed,
        CALPADSSchoolCode,
        CALPADSSchoolName,
        Section504Status,
        CALPADSSpecialEducation,
        SpecialEducationForTesting,
        PrimaryDisabilityType,
        PrimaryDisabilityforTesting,
        MigrantStatus,
        ELEntryDate,
        ELExitDate,
        ELStatus,
        FirstEntryDateInUSSchool,
        ELASforTesting,
        CALPADSPrimaryLanguage,
        PrimaryLanguageforTesting,
        CEDSLanguageCode,
        MilitaryStatus,
        FosterStatus,
        HomelessStatus,
        EconomicDisadvantageStatus,
        EconomicDisadvantageTesting,
        ReportingEthnicity,
        ParentEducationLevel,
        TestedStatus,
        FinalTestedSchoolCode,
        FinalTestedCompletedDate,
        StudentExitCode,
        StudentExitDate,
        StudentExitFileDate,
        ConditionCode,
        Attemptedness,
        IncludeIndicator,
        OverallScaleScore,
        OverallPL,
        OralLanguagePL,
        WrittenLanguagePL,
        ListeningPL,
        SpeakingPL,
        WritingPL,
        ReadingPL,
        AttemptednessMinus1,
        GradeAssessedMinus1,
        OverallScaleScoreMinus1,
        OverallPLMinus1,
        OralLanguagePLMinus1,
        WrittenLanguagePLMinus1,
        ListeningPLMinus1,
        SpeakingPLMinus1,
        ReadingPLMinus1,
        WritingPLMinus1,
        AttemptednessMinus2,
        GradeAssessedMinus2,
        OverallScaleScoreMinus2,
        OverallPLMinus2,
        OralLanguagePLMinus2,
        WrittenLanguagePLMinus2,
        ListeningPLMinus2,
        SpeakingPLMinus2,
        ReadingPLMinus2,
        WritingPLMinus2,
        AttemptednessMinus3,
        GradeAssessedMinus3,
        OverallScaleScoreMinus3,
        OverallPLMinus3,
        OralLanguagePLMinus3,
        WrittenLanguagePLMinus3,
        ListeningPLMinus3,
        SpeakingPLMinus3,
        ReadingPLMinus3,
        WritingPLMinus3,
        ElpiLevel,
        ElpiLevelNumeric,
        ElpiLevelRank,
        ElpiLevelChange,
        ElpiChangeCategory,
        ElpiProgress,
        ElpiLevelMinus1,
        ElpiLevelNumericMinus1,
        ElpiLevelRankMinus1,
        ElpiLevelChangeMinus1,
        ElpiChangeCategoryMinus1,
        ElpiProgressMinus1,
        ElpiLevelMinus2,
        ElpiLevelNumericMinus2,
        ElpiLevelRankMinus2,
        ElpiLevelChangeMinus2,
        ElpiChangeCategoryMinus2,
        ElpiProgressMinus2,
        ElpiLevelMinus3,
        ElpiLevelNumericMinus3,
        ElpiLevelRankMinus3
    from elpi_change_added
)

select * from final
