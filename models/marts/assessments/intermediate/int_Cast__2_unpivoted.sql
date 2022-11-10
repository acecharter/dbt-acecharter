{{ config(
    materialized='table'
)}}

with unpivoted as (
  {{ dbt_utils.unpivot(
    relation=ref('int_Cast__1_filtered'),
    cast_to='STRING',
    exclude=[
      'AssessmentId',
      'AceAssessmentId',
      'AceAssessmentName',
      'AssessmentSubject',
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
      'TypeId',
      'DemographicId',
      'GradeLevel',
      'TestId',
      'StudentsEnrolled',
      'StudentsWithScores'
    ],
    remove=[
      'TestType',
      'TotalTestedWithScoresAtReportingLevel',
      'StudentsTested',
      'TypeId'
    ],
    field_name='ReportingMethod',
    value_name='SchoolResult'
  ) }}
),

final as (
  select
    * except(ReportingMethod),
    case
      when ReportingMethod like 'Mean%' OR ReportingMethod like 'PctStandard%' then 'Overall'
      when ReportingMethod like 'LifeSciences%' then 'Life Sciences Domain'
      when ReportingMethod like 'PhysicalSciences%' then 'Physical Sciences Domain'
      when ReportingMethod like 'EarthAndSpaceSciences%' then 'Earth and Space Sciences Domain'
    end as AssessmentObjective,
    case
      when ReportingMethod = 'MeanScaleScore' then 'Mean Scale Score'
      when ReportingMethod = 'MeanDistanceFromStandard' then 'Mean Distance From Standard'
      when ReportingMethod = 'PctStandardMetAndAbove' then 'Percent Met and Above'
      when ReportingMethod = 'PctStandardExceeded' then 'Percent Exceeded'
      when ReportingMethod = 'PctStandardMet' then 'Percent Met'
      when ReportingMethod = 'PctStandardNearlyMet' then 'Percent Nearly Met'
      when ReportingMethod = 'PctStandardNotMet' then 'Percent Not Met'
      when ReportingMethod like '%AboveStandard' then 'Percent Above Standard'
      when ReportingMethod like '%NearStandard' then 'Percent Near Standard'
      when ReportingMethod like '%BelowStandard' then 'Percent Below Standard'
    end as ReportingMethod,
    'FLOAT64' as ResultDataType,
    case
      when ReportingMethod like 'Mean%' then StudentsWithScores 
      else round(StudentsWithScores * cast(SchoolResult as float64), 0)
    end as StudentWithResultCount
  from unpivoted
  where SchoolResult is not null
)

select assessmentobjective, reportingmethod, count(*) from final group by 1, 2
