with assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId
    from {{ ref('stg_GSD__Assessments') }}
    where SystemOrVendorName = 'CAASPP'
),
    
entities as (
    select * from {{ ref('dim_Entities')}}
),
    
min_met_scores as (
    select
        AceAssessmentId,
        GradeLevel,
        MinStandardMetScaleScore
    from {{ ref('fct_CaasppMinMetScaleScores') }}
    where Area = 'Overall'
),

caaspp_unioned as (
    select * from {{ ref('base_RD__Caaspp2015')}}
    union all
    select * from {{ ref('base_RD__Caaspp2016')}}
    union all
    select * from {{ ref('base_RD__Caaspp2017')}}
    union all
    select * from {{ ref('base_RD__Caaspp2018')}}
    union all
    select * from {{ ref('base_RD__Caaspp2019')}}
    union all
    select * from {{ ref('base_RD__Caaspp2021')}}
    union all
    select * from {{ ref('base_RD__Caaspp2022')}}
),

caaspp_entity_codes_added as (
    select
        *,
        case
            when DistrictCode = '00000' then CountyCode
            when SchoolCode = '0000000' then DistrictCode
            else SchoolCode
        end as EntityCode
    from caaspp_unioned
),

caaspp_filtered as (
    select
        assessment_ids.AceAssessmentId,
        assessment_ids.AceAssessmentName,
        assessment_ids.AssessmentSubject,
        entities.*,
        caaspp_entity_codes_added.* except(EntityCode, Filler)
    from caaspp_entity_codes_added
    left join assessment_ids
    on caaspp_entity_codes_added.TestId = assessment_ids.SystemOrVendorAssessmentId
    left join entities
    on caaspp_entity_codes_added.EntityCode = entities.EntityCode
    WHERE caaspp_entity_codes_added.EntityCode in (select EntityCode from entities)
),

caaspp_sy_and_dfs_added as (
    select
        caaspp_filtered.*,
        concat(cast(caaspp_filtered.TestYear - 1 as string), '-', cast(caaspp_filtered.TestYear - 2000 as string) ) as SchoolYear,
        cast(
            case when caaspp_filtered.MeanScaleScore is not null then round(caaspp_filtered.MeanScaleScore - min_met_scores.MinStandardMetScaleScore, 1) else null end as string
        ) as MeanDistancefromStandard
    from caaspp_filtered
    left join min_met_scores
    ON
        caaspp_filtered.AceAssessmentId = min_met_scores.AceAssessmentId
        AND cast(caaspp_filtered.GradeLevel as string) = min_met_scores.GradeLevel
),

final as (
    select
        AceAssessmentId,
        AceAssessmentName,
        AssessmentSubject,
        EntityCode,
        EntityType,
        EntityName,
        EntityNameMid,
        EntityNameShort,
        CountyCode,
        DistrictCode,
        SchoolCode,
        SchoolYear,
        TestYear,
        DemographicId,
        TestType,
        TotalTestedAtReportingLevel
        TotalTestedWithScoresAtReportingLevel,
        GradeLevel,
        TestId,
        StudentsEnrolled,
        StudentsTested,
        MeanScaleScore,
        round(PctStandardExceeded / 100, 4) as PctStandardExceeded,
        round(PctStandardMet / 100, 4) as PctStandardMet,
        round(PctStandardMetAndAbove / 100, 4) as PctStandardMetAndAbove,
        round(PctStandardNearlyMet / 100, 4) as PctStandardNearlyMet,
        round(PctStandardNotMet / 100, 4) as PctStandardNotMet,
        StudentsWithScores,
        round(Area1PctAboveStandard / 100, 4) as Area1PctAboveStandard,
        round(Area1PctNearStandard / 100, 4) as Area1PctNearStandard,
        round(Area1PctBelowStandard / 100, 4) as Area1PctBelowStandard,
        round(Area2PctAboveStandard / 100, 4) as Area2PctAboveStandard,
        round(Area2PctNearStandard / 100, 4) as Area2PctNearStandard,
        round(Area2PctBelowStandard / 100, 4) as Area2PctBelowStandard,
        round(Area3PctAboveStandard / 100, 4) as Area3PctAboveStandard,
        round(Area3PctNearStandard / 100, 4) as Area3PctNearStandard,
        round(Area3PctBelowStandard / 100, 4) as Area3PctBelowStandard,
        round(Area4PctAboveStandard / 100, 4) as Area4PctAboveStandard,
        round(Area4PctNearStandard / 100, 4) as Area4PctNearStandard,
        round(Area4PctBelowStandard / 100, 4) as Area4PctBelowStandard,
        TypeId,
        MeanDistancefromStandard
    from caaspp_sy_and_dfs_added
)

select * from final