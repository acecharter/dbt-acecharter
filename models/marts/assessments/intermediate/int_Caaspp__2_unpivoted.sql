{{ config(
    materialized='table'
)}}

WITH unpivoted as (
    {{ dbt_utils.unpivot(
      relation=ref('int_Caaspp__1_filtered'),
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

  final AS (
    SELECT
      *,
      CASE
        WHEN ReportingMethod LIKE 'Mean%' THEN StudentsWithScores 
        ELSE ROUND(StudentsWithScores * CAST(SchoolResult AS FLOAT64), 0)
      END AS StudentWithResultCount
    FROM unpivoted
  )
  
SELECT * FROM final
