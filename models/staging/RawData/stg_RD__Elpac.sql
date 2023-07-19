{{ config(
    materialized='table'
)}}

with entities as (
    select * from {{ ref('dim_Entities')}}
),

assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        SystemOrVendorAssessmentId
    from {{ ref('stg_GSD__Assessments') }}
    where SystemOrVendorName = 'ELPAC'
),

elpac_unioned as (
    select * from {{ ref('base_RD__Elpac2018')}}
    union all
    select * from {{ ref('base_RD__Elpac2019')}}
    union all
    select * from {{ ref('base_RD__Elpac2021')}}
    union all
    select * from {{ ref('base_RD__Elpac2022')}}
),

elpac_entity_codes_added as (
    select
        a.AceAssessmentId,
        a.AceAssessmentName,
        case
            when u.DistrictCode = '00000' then u.CountyCode
            when u.SchoolCode = '0000000' then u.DistrictCode
            else u.SchoolCode
        end as EntityCode,
        u.*
    from elpac_unioned as u
    left join assessment_ids as a
    on u.AssessmentType = a.SystemOrVendorAssessmentId
),

elpac_filtered as (
    select *
    from elpac_entity_codes_added
    where EntityCode in (select EntityCode from entities)
        
),

elpac_sy_entity_info_added as (
    select
        entities.* except (EntityCode), 
        concat(
            cast(e.TestYear - 1 as string), '-', cast(e.TestYear - 2000 as string)
        ) as SchoolYear,
        e.*
    from elpac_filtered as e
    left join entities
    on e.EntityCode = entities.EntityCode
),

final as (
    select
        AceAssessmentId,
        AceAssessmentName,
        EntityCode,
        EntityType,
        EntityName,
        EntityNameMid,
        EntityNameShort,
        CountyCode,
        DistrictCode,
        SchoolCode,
        RecordType,
        CharterNumber,
        SchoolYear,
        * except(
            AceAssessmentId,
            AceAssessmentName,
            EntityCode,
            EntityType,
            EntityName,
            EntityNameMid,
            EntityNameShort,
            CountyCode,
            DistrictCode,
            SchoolCode,
            RecordType,
            CharterNumber,
            SchoolYear
        )
    from elpac_sy_entity_info_added
)

select * from final
