with unpivoted as (
  {{ dbt_utils.unpivot(
    relation=ref('stg_RenaissanceStar'),
    cast_to='STRING',
    exclude=[
      'AssessmentId',
      'AceAssessmentId',
      'AssessmentName',
      'AssessmentSubject',
      'TestedSchoolId',
      'SchoolYear',
      'StudentRenaissanceID',
      'StudentIdentifier',
      'StateUniqueId',
      'StarTestingWindow',
      'AssessmentDate',
      'GradeLevel',
      'AssessmentGradeLevel',
      'GradePlacement'
    ],
    remove=[
      'TestedSchoolName',
      'DisplayName',
      'LastName',
      'FirstName',
      'MiddleName',
      'Gender',
      'BirthDate',
      'EnrollmentStatus',
      'AssessmentNumber',
      'AssessmentType',
      'TotalTimeInSeconds',
      'CurrentSGP',
      'AceTestingWindowName',
      'AceTestingWindowStartDate',
      'AceTestingWindowEndDate',
      'StarTestingWindow',
      'ScaledScore'
    ],
    field_name='ReportingMethod',
    value_name='StudentResult'
) }}
),

final as (
  select * except(ReportingMethod, StudentResult),
  case
    when ReportingMethod='GradeEquivalent' then 'Grade Equivalent'
    when ReportingMethod='UnifiedScore' then 'Unified Score'
    when ReportingMethod='PercentileRank' then 'Percentile Rank'
    when ReportingMethod='NormalCurveEquivalent' then 'Normal Curve Equivalent'
    when ReportingMethod='StudentGrowthPercentileFallFall' then 'SGP (Fall to Fall)'
    when ReportingMethod='StudentGrowthPercentileFallWinter' then 'SGP (Fall to Winter)'
    when ReportingMethod='StudentGrowthPercentileFallSpring' then 'SGP (Fall to Spring)'
    when ReportingMethod='StudentGrowthPercentileSpringSpring' then 'SGP (Spring to Spring)'
    when ReportingMethod='StudentGrowthPercentileWinterSpring' then 'SGP (Winter to Spring)'
    when ReportingMethod='StateBenchmarkCategoryLevel' then 'State Benchmark Level'
    when ReportingMethod='LiteracyClassification' then 'Literacy Classification'
    when ReportingMethod='InstructionalReadingLevel' then 'Instructional Reading Level'
    else ReportingMethod
  end as ReportingMethod,
  case
    when ReportingMethod in ('GradeEquivalent', 'Lexile', 'LiteracyClassification', 'Quantile', 'InstructionalReadingLevel') then 'STRING'
    when ReportingMethod in ('UnifiedScore', 'PercentileRank','StateBenchmarkCategoryLevel') or ReportingMethod like 'StudentGrowth%' then 'INT64'
    when ReportingMethod = 'NormalCurveEquivalent' then 'FLOAT64'
  end as StudentResultDataType,
  StudentResult
  from unpivoted
  where StudentResult is not null
)

select * from final