with caaspp as (
    select * from {{ ref('base_RD__TomsCaasppTested2023')}}
    union all
    select * from {{ ref('base_RD__TomsCaasppTested2022')}}
),

race_ethnicity as (
    select * from {{ ref('stg_RD__TomsEthnicityCodes')}}
),

assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId
    from {{ ref('stg_GSD__Assessments') }}
    where SystemOrVendorName = 'CAASPP'
),

min_met_scores as (
    select
        AceAssessmentId,
        GradeLevel,
        MinStandardMetScaleScore
    from {{ ref('fct_CaasppMinMetScaleScores') }}
    where Area='Overall'
),

caaspp_id_race_added as (
    select
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.AssessmentSubject,
        c.*,
        r.RaceEthnicity
    from caaspp as c
    left join assessment_ids as a
    on cast(cast(c.RecordType as int64) as string) = a.SystemOrVendorAssessmentId
    left join race_ethnicity as r
    on c.ReportingEthnicity = r.RaceEthnicityCode
),

rfep as (
    select * from {{ ref('stg_RD__Calpads217Elas')}}
),

caaspp_rfep_updated as (
    select
        caaspp.AceAssessmentId,
        caaspp.AceAssessmentName,
        caaspp.AssessmentSubject,
        caaspp.TestYear,
        caaspp.SchoolYear,
        caaspp.RecordType,
        caaspp.SSID,
        caaspp.StudentLastName,
        caaspp.StudentFirstName,
        caaspp.StudentMiddleName,
        caaspp.DateofBirth,
        caaspp.Gender,
        caaspp.GradeAssessed,
        caaspp.CALPADSSchoolCode,
        caaspp.CALPADSSchoolName,
        caaspp.Section504Status,
        caaspp.CALPADSIDEAIndicator,
        caaspp.IDEAIndicatorForTesting,
        caaspp.MigrantStatus,
        caaspp.ELStatus,
        caaspp.ELEntryDate,
        coalesce(caaspp.RFEPDate, rfep.RFEPDate) as RFEPDate,
        caaspp.FirstEntryDateInUSSchool,
        caaspp.EnrollmentEffectiveDate,
        caaspp.ELAS,
        caaspp.CEDSLanguageCode,
        caaspp.CALPADSPrimaryLanguage,
        caaspp.MilitaryStatus,
        caaspp.FosterStatus,
        caaspp.EconomicDisadvantageStatus,
        caaspp.EconomicDisadvantageTesting,
        caaspp.CALPADSNPSSchoolFlag,
        caaspp.HispanicorLatino,
        caaspp.AmericanIndianorAlaskaNative,
        caaspp.Asian,
        caaspp.HawaiianOrOtherPacificIslander,
        caaspp.Filipino,
        caaspp.BlackorAfricanAmerican,
        caaspp.White,
        caaspp.TwoorMoreRaces,
        caaspp.ReportingEthnicity,
        caaspp.FinalTestedSchoolCode,
        caaspp.StudentExitCode,
        caaspp.StudentExitWithdrawalDate,
        caaspp.StudentRemovedCALPADSFileDate,
        caaspp.ConditionCode,
        caaspp.Attemptedness,
        caaspp.ScoreStatus,
        caaspp.IncludeIndicator,
        caaspp.LexileorQuantileMeasure,
        caaspp.GrowthScore,
        caaspp.ScaleScore,
        caaspp.AchievementLevels,
        caaspp.GradeAssessedMinus1,
        caaspp.ScaleScoreMinus1,
        caaspp.AchievementLevelMinus1,
        caaspp.GradeAssessedMinus2,
        caaspp.ScaleScoreMinus2,
        caaspp.AchievementLevelMinus2,
        caaspp.GradeAssessedMinus3,
        caaspp.ScaleScoreMinus3,
        caaspp.AchievementLevelMinus3,
        caaspp.RaceEthnicity,
        case
            when caaspp.SchoolYear not in (select distinct SchoolYear from rfep) then null
            when 
                caaspp.RFEPDate is null 
                and rfep.RFEPDate is not null
            then 'Y' 
            else 'N'
        end as EndOfYearRfep
    from caaspp_id_race_added as caaspp
    left join rfep
    on caaspp.CALPADSSchoolCode = rfep.SchoolCode
    and caaspp.SSID = rfep.SSID
    and caaspp.SchoolYear = rfep.SchoolYear
),

final as (
    select
        c.*,
        case
        when ELStatus is true or date(RFEPDate) > date(TestYear - 4, 6, 15) then 'Y' else 'N' end as ElWithinPast4Years,
        cast(
            case when c.ScaleScore is not null then round(c.ScaleScore - m.MinStandardMetScaleScore, 0) else null end as int64
        ) as Dfs,
        cast(
            case when c.ScaleScoreMinus1 is not null then round(c.ScaleScoreMinus1 - m1.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus1,
        cast(
            case when c.ScaleScoreMinus2 is not null then round(c.ScaleScoreMinus2 - m2.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus2,
        cast(
            case when c.ScaleScoreMinus3 is not null then round(c.ScaleScoreMinus3 - m3.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus3
    from caaspp_rfep_updated as c
    left join min_met_scores as m
    on
        c.AceAssessmentId = m.AceAssessmentId
        and c.GradeAssessed = m.GradeLevel
    left join min_met_scores as m1
    on
        c.AceAssessmentId = m1.AceAssessmentId
        and c.GradeAssessedMinus1 = m1.GradeLevel
    left join min_met_scores as m2
    on
        c.AceAssessmentId = m2.AceAssessmentId
        and c.GradeAssessedMinus2 = m2.GradeLevel
    left join min_met_scores as m3
    on
        c.AceAssessmentId = m3.AceAssessmentId
        and c.GradeAssessedMinus3 = m3.GradeLevel
)

select * from final
