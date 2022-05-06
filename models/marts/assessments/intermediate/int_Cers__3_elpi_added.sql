WITH
  elpi_levels AS (
    SELECT * FROM {{ ref('stg_GSD__ElpiLevels') }}
  ),

  cers AS (
    SELECT * FROM {{ ref('int_Cers__2_melted')}}
  ),

  elpac_scale_scores AS (
    SELECT * 
    FROM cers
    WHERE
      AceAssessmentId = '8'
      AND AssessmentObjective = 'Overall'
      AND ReportingMethod = 'Scale Score'
  ),

  elpi_results AS (
    SELECT
      s.* EXCEPT(
        ReportingMethod,
        StudentResultDataType,
        StudentResult
      ),
      'ELPI Level' AS ReportingMethod,
      'STRING' AS StudentResultDataType,
      e.ElpiLevel AS StudentResult
    FROM elpac_scale_scores AS s
    LEFT JOIN elpi_levels AS e
    ON
      s.AssessmentGradeLevel = CAST(e.GradeLevel AS STRING) AND
      CAST(s.StudentResult AS INT64) BETWEEN CAST(e.MinScaleScore AS INT64) AND CAST(e.MaxScaleScore AS INT64)
  ),

  final AS (
    SELECT * FROM cers
    UNION ALL
    SELECT * FROM elpi_results
  )

SELECT * FROM final