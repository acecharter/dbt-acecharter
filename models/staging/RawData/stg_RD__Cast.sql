{{ config(
    materialized='table'
)}}

with assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject
    from {{ ref('stg_GSD__Assessments') }}
    where AssessmentNameShort = 'CAST'
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
    where Area='Overall'
),

cast_unioned as (
    select * from {{ ref('base_RD__Cast2019')}}
    union all
    select * from {{ ref('base_RD__Cast2021')}}
    union all
    select * from {{ ref('base_RD__Cast2022')}}
    union all
    select * from {{ ref('base_RD__Cast2023')}}
),

cast_entity_codes_added as (
    select
        *,
        case
            when DistrictCode = '00000' then CountyCode
            when SchoolCode = '0000000' then DistrictCode
            else SchoolCode
        end as EntityCode
    from cast_unioned
),

cast_filtered as (
    select
        assessment_ids.AceAssessmentId,
        assessment_ids.AceAssessmentName,
        assessment_ids.AssessmentSubject,
        entities.*,
        cast_entity_codes_added.* except(EntityCode, Filler)
    from cast_entity_codes_added
    cross join assessment_ids
    left join entities
    on cast_entity_codes_added.EntityCode = entities.EntityCode
    where cast_entity_codes_added.EntityCode in (select EntityCode from entities)
),

cast_sy_and_dfs_added as (
    select
        cast_filtered.*,
        concat(cast(cast_filtered.TestYear - 1 as string), '-', cast(cast_filtered.TestYear - 2000 as string) ) as SchoolYear,
        cast(
            case when cast_filtered.MeanScaleScore is not null then round(cast_filtered.MeanScaleScore - min_met_scores.MinStandardMetScaleScore, 1) else null end as string
        ) as MeanDistanceFromStandard
    from cast_filtered
    left join min_met_scores
    on
        cast_filtered.AceAssessmentId = min_met_scores.AceAssessmentId
        and cast(cast_filtered.GradeLevel as string) = min_met_scores.GradeLevel
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
        round(LifeSciencesDomainPercentBelowStandard / 100, 4) as LifeSciencesDomainPercentBelowStandard,
        round(LifeSciencesDomainPercentNearStandard / 100, 4) as LifeSciencesDomainPercentNearStandard,
        round(LifeSciencesDomainPercentAboveStandard / 100, 4) as LifeSciencesDomainPercentAboveStandard,
        round(PhysicalSciencesDomainPercentBelowStandard / 100, 4) as PhysicalSciencesDomainPercentBelowStandard,
        round(PhysicalSciencesDomainPercentNearStandard / 100, 4) as PhysicalSciencesDomainPercentNearStandard,
        round(PhysicalSciencesDomainPercentAboveStandard / 100, 4) as PhysicalSciencesDomainPercentAboveStandard,
        round(EarthAndSpaceSciencesDomainPercentBelowStandard / 100, 4) as EarthAndSpaceSciencesDomainPercentBelowStandard,
        round(EarthAndSpaceSciencesDomainPercentNearStandard / 100, 4) as EarthAndSpaceSciencesDomainPercentNearStandard,
        round(EarthAndSpaceSciencesDomainPercentAboveStandard / 100, 4) as EarthAndSpaceSciencesDomainPercentAboveStandard,
        TypeId,
        MeanDistanceFromStandard
    from cast_sy_and_dfs_added
)

select * from final
