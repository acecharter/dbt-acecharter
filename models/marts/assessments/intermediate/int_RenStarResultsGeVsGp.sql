WITH results AS (
  SELECT * FROM {{ ref('int_RenStar__unioned')}}
),

ge_values AS (
  SELECT
    AssessmentId,
    CASE
      WHEN GradeEquivalent = '<1' OR GradeEquivalent = '< 1' THEN 0.9
      WHEN GradeEquivalent = '> 9' THEN 9.1
      WHEN GradeEquivalent = '> 10' THEN 10.1
      WHEN GradeEquivalent = '> 11' THEN 11.1
      WHEN GradeEquivalent = '> 12' THEN 12.1
      WHEN GradeEquivalent = '> 12.9' THEN 13.0
      ELSE CAST(GradeEquivalent AS FLOAT64)
    END AS GradeEquivalentValue,
  FROM results
),

ge_minus_gp AS (
  SELECT
    r.AssessmentId,
    ROUND(GradeEquivalentValue - r.GradePlacement, 2) AS GeMinusGp
  FROM results AS r
  LEFT JOIN ge_values AS gv
  USING (AssessmentId)
),

ge_minus_gp_categories AS (
  SELECT
    AssessmentId,
    CASE
      WHEN GeMinusGp > 0 THEN 'Above GP'
      WHEN GeMinusGp = 0 THEN 'At GP'
      WHEN GeMinusGp < 0 THEN 'Below GP'
    END AS GeRelativeToGpCategory
  FROM ge_minus_gp
)

SELECT
  r.AssessmentId,
  r.GradeEquivalent,
  gv.GradeEquivalentValue,
  r.GradePlacement,
  gmg.GeMinusGp,
  c.GeRelativeToGpCategory
FROM results AS r
LEFT JOIN ge_values AS gv
ON r.AssessmentId = gv.AssessmentId
LEFT JOIN ge_minus_gp AS gmg
ON r.AssessmentId = gmg.AssessmentId
LEFT JOIN ge_minus_gp_categories AS c
ON r.AssessmentId = c.AssessmentId