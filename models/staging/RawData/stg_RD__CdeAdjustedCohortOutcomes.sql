WITH acgr_2017 as (
    select * from {{ source('RawData', 'CdeAdjustedCohortOutcomes2017')}}
),

acgr_2018 as (
    select * from {{ source('RawData', 'CdeAdjustedCohortOutcomes2018')}}
),

acgr_2019 as (
    select * from {{ source('RawData', 'CdeAdjustedCohortOutcomes2019')}}
),

acgr_2020 as (
    select * from {{ source('RawData', 'CdeAdjustedCohortOutcomes2020')}}
),

acgr_2021 as (
    select * from {{ source('RawData', 'CdeAdjustedCohortOutcomes2021')}}
),

acgr_2022 as (
    select * from {{ ref('base_RD__CdeAdjustedCohortOutcomes2022')}}
),

unioned as (
    select * from acgr_2017
    union all
    select * from acgr_2018
    union all
    select * from acgr_2019
    union all
    select * from acgr_2020
    union all
    select * from acgr_2021
    union all
    select * from acgr_2022
),

final as (
    select
        AcademicYear,
        AggregateLevel,
        case AggregateLevel
            when 'T' then 'State'
            when 'C' then 'County'
            when 'D' then 'District'
            when 'S' then 'School'
        end as EntityType,
        format("%02d", cast(CountyCode as int64)) as CountyCode,
        format("%05d", cast(DistrictCode as int64)) as DistrictCode,
        format("%07d", cast(SchoolCode as int64)) as SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        TRIM(CharterSchool) as CharterSchool,
        TRIM(DASS) as DASS,
        ReportingCategory,
        CohortStudents,
        Regular_HS_Diploma_Graduates__Count_ as RegularHsDiplomaGraduatesCount,
        Regular_HS_Diploma_Graduates__Rate_ as RegularHsDiplomaGraduatesRate,
        Met_UC_CSU_Grad_Req_s__Count_ as MetUcCsuGradReqsCount,
        Met_UC_CSU_Grad_Req_s__Rate_ as MetUcCsuGradReqsRate,
        Seal_of_Biliteracy__Count_ as SealOfBiliteracyCount,
        Seal_of_Biliteracy__Rate_ as SealOfBiliteracyRate,
        Golden_State_Seal_Merit_Diploma__Count_ as GoldenStateSealMeritDiplomaCount,
        Golden_State_Seal_Merit_Diploma__Rate as GoldenStateSealMeritDiplomaRate,
        CHSPE_Completer__Count_ as ChspeCompleterCount,
        CHSPE_Completer__Rate_ as ChspeCompleterRate,
        Adult_Ed__HS_Diploma__Count_ as AdultEdHsDiplomaCount,
        Adult_Ed__HS_Diploma__Rate_ as AdultEdHsDiplomaRate,
        SPED_Certificate__Count_ as SpedCertificateCount,
        SPED_Certificate__Rate_ as SpedCertificateRate,
        GED_Completer__Count_ as GedCompleterCount,
        GED_Completer__Rate_ as GedCompleterRate,
        Other_Transfer__Count_ as OtherTransferCount,
        Other_Transfer__Rate_ as OtherTransferRate,
        Dropout__Count_ as DropoutCount,
        Dropout__Rate_ as DropoutRate,
        Still_Enrolled__Count_ as StillEnrolledCount,
        Still_Enrolled__Rate_ as StillEnrolledRate
    from unioned
    where
        AggregateLevel = 'T'
        or (
            AggregateLevel = 'C' 
            AND CountyCode = 43
        )
        or DistrictCode = 69427
)

select * from final
