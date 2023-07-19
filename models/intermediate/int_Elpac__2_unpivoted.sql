{{ config(
    materialized='table'
)}}

with unpivoted as (
    {{ dbt_utils.unpivot(
        relation=ref('int_Elpac__1_filtered'),
        cast_to='STRING',
        exclude=[
            'AssessmentId',
            'AceAssessmentId',
            'AceAssessmentName',
            'EntityCode',
            'EntityType',
            'EntityName',
            'EntityNameMid',
            'EntityNameShort',
            'CountyCode',
            'DistrictCode',
            'SchoolCode',
            'SchoolYear',
            'TestYear',
            'RecordType',
            'StudentGroupId',
            'GradeLevel',
            'AssessmentType',
            'TotalEnrolled',
            'TotalTestedWithScores',
            'OverallPerfLvl1Count',
            'OverallPerfLvl2Count',
            'OverallPerfLvl3Count',
            'OverallPerfLvl4Count',
            'OverallTotal',
            'OralLangPerfLvl1Count',
            'OralLangPerfLvl2Count',
            'OralLangPerfLvl3Count',
            'OralLangPerfLvl4Count',
            'OralLangTotal',
            'WritLangPerfLvl1Count',
            'WritLangPerfLvl2Count',
            'WritLangPerfLvl3Count',
            'WritLangPerfLvl4Count',
            'WritLangTotal',
            'ListeningDomainBeginCount',
            'ListeningDomainModerateCount',
            'ListeningDomainDevelopedCount',
            'SpeakingDomainBeginCount',
            'SpeakingDomainModerateCount',
            'SpeakingDomainDevelopedCount',
            'ReadingDomainBeginCount',
            'ReadingDomainModerateCount',
            'ReadingDomainDevelopedCount',
            'WritingDomainBeginCount',
            'WritingDomainModerateCount',
            'WritingDomainDevelopedCount'
        ],
        remove=[
            'CharterNumber',
            'TotalTested',
            'ListeningDomainTotal',
            'SpeakingDomainTotal',
            'ReadingDomainTotal',
            'WritingDomainTotal'
        ],
        field_name='ReportingMethod',
        value_name='SchoolResult'
    ) }}
),

final as (
  select
    * except(
        ReportingMethod,
        OverallPerfLvl1Count,
        OverallPerfLvl2Count,
        OverallPerfLvl3Count,
        OverallPerfLvl4Count,
        OverallTotal,
        OralLangPerfLvl1Count,
        OralLangPerfLvl2Count,
        OralLangPerfLvl3Count,
        OralLangPerfLvl4Count,
        OralLangTotal,
        WritLangPerfLvl1Count,
        WritLangPerfLvl2Count,
        WritLangPerfLvl3Count,
        WritLangPerfLvl4Count,
        WritLangTotal,
        ListeningDomainBeginCount,
        ListeningDomainModerateCount,
        ListeningDomainDevelopedCount,
        SpeakingDomainBeginCount,
        SpeakingDomainModerateCount,
        SpeakingDomainDevelopedCount,
        ReadingDomainBeginCount,
        ReadingDomainModerateCount,
        ReadingDomainDevelopedCount,
        WritingDomainBeginCount,
        WritingDomainModerateCount,
        WritingDomainDevelopedCount
    ),
    case
        when ReportingMethod like 'Overall%' then 'Overall'
        when ReportingMethod like 'OralLang%' then 'Oral Language'
        when ReportingMethod like 'WritLang%' then 'Written Language'
        when ReportingMethod like 'Listening%' then 'Listening Domain'
        when ReportingMethod like 'Speaking%' then 'Speaking Domain'
        when ReportingMethod like 'Reading%' then 'Reading Domain'
        when ReportingMethod like 'Writing%' then 'Writing Domain'
    end as AssessmentObjective,
    case
        when ReportingMethod like '%MeanSclScr' then 'Mean Scale Score'
        when ReportingMethod like '%Lvl1%' then 'Percent Level 1'
        when ReportingMethod like '%Lvl2%' then 'Percent Level 2'
        when ReportingMethod like '%Lvl3%' then 'Percent Level 3'
        when ReportingMethod like '%Lvl4%' then 'Percent Level 4'
        when ReportingMethod like '%Begin%' then 'Percent Beginning to Develop'
        when ReportingMethod like '%Moderate%' then 'Percent Somewhat/Moderately Developed'
        when ReportingMethod like '%Developed%' then 'Percent Well Developed'
    end as ReportingMethod,
    'FLOAT64' as ResultDataType,
    case
        when ReportingMethod = 'OverallMeanSclScr' then OverallTotal
        when ReportingMethod = 'OverallPerfLvl1Pcnt' then OverallPerfLvl1Count
        when ReportingMethod = 'OverallPerfLvl2Pcnt' then OverallPerfLvl2Count
        when ReportingMethod = 'OverallPerfLvl3Pcnt' then OverallPerfLvl3Count
        when ReportingMethod = 'OverallPerfLvl4Pcnt' then OverallPerfLvl4Count
        when ReportingMethod = 'OralLangMeanSclScr' then OralLangTotal
        when ReportingMethod = 'OralLangPerfLvl1Pcnt' then OralLangPerfLvl1Count
        when ReportingMethod = 'OralLangPerfLvl2Pcnt' then OralLangPerfLvl2Count
        when ReportingMethod = 'OralLangPerfLvl3Pcnt' then OralLangPerfLvl3Count
        when ReportingMethod = 'OralLangPerfLvl4Pcnt' then OralLangPerfLvl4Count
        when ReportingMethod = 'WritLangMeanSclScr' then WritLangTotal
        when ReportingMethod = 'WritLangPerfLvl1Pcnt' then WritLangPerfLvl1Count
        when ReportingMethod = 'WritLangPerfLvl2Pcnt' then WritLangPerfLvl2Count
        when ReportingMethod = 'WritLangPerfLvl3Pcnt' then WritLangPerfLvl3Count
        when ReportingMethod = 'WritLangPerfLvl4Pcnt' then WritLangPerfLvl4Count
        when ReportingMethod = 'ListeningDomainBeginPcnt' then ListeningDomainBeginCount
        when ReportingMethod = 'ListeningDomainModeratePcnt' then ListeningDomainModerateCount
        when ReportingMethod = 'ListeningDomainDevelopedPcnt' then ListeningDomainDevelopedCount
        when ReportingMethod = 'SpeakingDomainBeginPcnt' then SpeakingDomainBeginCount
        when ReportingMethod = 'SpeakingDomainModeratePcnt' then SpeakingDomainModerateCount
        when ReportingMethod = 'SpeakingDomainDevelopedPcnt' then SpeakingDomainDevelopedCount
        when ReportingMethod = 'ReadingDomainBeginPcnt' then ReadingDomainBeginCount
        when ReportingMethod = 'ReadingDomainModeratePcnt' then ReadingDomainModerateCount
        when ReportingMethod = 'ReadingDomainDevelopedPcnt' then ReadingDomainDevelopedCount
        when ReportingMethod = 'WritingDomainBeginPcnt' then WritingDomainBeginCount
        when ReportingMethod = 'WritingDomainModeratePcnt' then WritingDomainModerateCount
        when ReportingMethod = 'WritingDomainDevelopedPcnt' then WritingDomainDevelopedCount
    end as StudentWithResultCount
  from unpivoted
  where SchoolResult is not null
)

select * from final