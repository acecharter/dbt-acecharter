with caaspp as (
    select * from {{ ref('stg_RD__TomsCaasppTested')}}
),

rfep as (
    select * from {{ ref('stg_RD__Calpads217Elas')}}
),

final as (
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
        caaspp.ElWithinPast4Years,
        caaspp.Dfs,
        caaspp.DfsMinus1,
        caaspp.DfsMinus2,
        caaspp.DfsMinus3,
        case when caaspp.RFEPDate is null and rfep.RFEPDate is not null then 'Yes' else 'No' end as EndOfYearRfep
    from caaspp
    left join rfep
    on caaspp.CALPADSSchoolCode = rfep.SchoolCode
    and caaspp.SSID = rfep.SSID
)

select * from final
