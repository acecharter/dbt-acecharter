WITH
  elpi_levels AS (
    SELECT * FROM {{ ref('stg_GSD__ElpiLevels') }}
  ),

  cers AS (
    SELECT * FROM {{ ref('int_Cers__1_unioned')}}
  ),

  final AS (
    SELECT
      c.*,
      CASE WHEN c.AceAssessmentId = '8' THEN e.ElpiLevel END AS ElpiLevel
    FROM cers AS c
    LEFT JOIN elpi_levels AS e
    ON
      CAST(c.GradeLevel AS INT64) = e.GradeLevel
      AND CAST(c.ScaleScore AS INT64) BETWEEN CAST(e.MinScaleScore AS INT64) AND CAST(e.MaxScaleScore AS INT64)
  )

SELECT * FROM final