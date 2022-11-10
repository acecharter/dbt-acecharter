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
      'AssessmentType'
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
      WritingDomainDevelopedCount,
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
      when ReportingMethod like '%Lvl1' then 'Percent Level 1'
      when ReportingMethod like '%Lvl2' then 'Percent Level 2'
      when ReportingMethod like '%Lvl3' then 'Percent Level 3'
      when ReportingMethod like '%Lvl4' then 'Percent Level 4'
      when ReportingMethod like '%Begin' then 'Percent Beginning to Develop'
      when ReportingMethod like '%Moderate' then 'Percent Somewhat/Moderately Developed'
      when ReportingMethod like '%Developed' then 'Percent Well Developed'
    end as ReportingMethod,
    'FLOAT64' as ResultDataType,
    case
      when ReportingMethod = 'OverallMeanSclScr' THEN OverallTotal
      when ReportingMethod = 'OverallPerfLvl1Pct' THEN OverallPerfLvl1Count
      when ReportingMethod = 'OverallPerfLvl2Pct' THEN OverallPerfLvl2Count
      when ReportingMethod = 'OverallPerfLvl3Pct' THEN OverallPerfLvl3Count
      when ReportingMethod = 'OverallPerfLvl4Pct' THEN OverallPerfLvl4Count
      when ReportingMethod = 'OralLangMeanSclScr' THEN OralLangTotal
      when ReportingMethod = 'OralLangPerfLvl1Pct' THEN OralLangPerfLvl1Count
      when ReportingMethod = 'OralLangPerfLvl2Pct' THEN OralLangPerfLvl2Count
      when ReportingMethod = 'OralLangPerfLvl3Pct' THEN OralLangPerfLvl3Count
      when ReportingMethod = 'OralLangPerfLvl4Pct' THEN OralLangPerfLvl4Count
      when ReportingMethod = 'WritLangMeanSclScr' THEN WritLangTotal
      when ReportingMethod = 'WritLangPerfLvl1Pct' THEN WritLangPerfLvl1Count
      when ReportingMethod = 'WritLangPerfLvl2Pct' THEN WritLangPerfLvl2Count
      when ReportingMethod = 'WritLangPerfLvl3Pct' THEN WritLangPerfLvl3Count
      when ReportingMethod = 'WritLangPerfLvl4Pct' THEN WritLangPerfLvl4Count
      when ReportingMethod = 'ListeningDomainBeginPct' THEN ListeningDomainBeginCount
      when ReportingMethod = 'ListeningDomainModeratePct' THEN ListeningDomainModerateCount
      when ReportingMethod = 'ListeningDomainDevelopedPct' THEN ListeningDomainDevelopedCount
      when ReportingMethod = 'SpeakingDomainBeginPct' THEN SpeakingDomainBeginCount
      when ReportingMethod = 'SpeakingDomainModeratePct' THEN SpeakingDomainModerateCount
      when ReportingMethod = 'SpeakingDomainDevelopedPct' THEN SpeakingDomainDevelopedCount
      when ReportingMethod = 'ReadingDomainBeginPct' THEN ReadingDomainBeginCount
      when ReportingMethod = 'ReadingDomainModeratePct' THEN ReadingDomainModerateCount
      when ReportingMethod = 'ReadingDomainDevelopedPct' THEN ReadingDomainDevelopedCount
      when ReportingMethod = 'WritingDomainBeginPct' THEN WritingDomainBeginCount
      when ReportingMethod = 'WritingDomainModeratePct' THEN WritingDomainModerateCount
      when ReportingMethod = 'WritingDomainDevelopedPct' THEN WritingDomainDevelopedCount
    end as StudentWithResultCount
  from unpivoted
  where SchoolResult is not null
)

select * from final