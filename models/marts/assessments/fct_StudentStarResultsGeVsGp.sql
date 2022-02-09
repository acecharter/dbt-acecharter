WITH star_assessments AS (
    SELECT *
    FROM {{ ref('fct_StudentAssessment')}}
    WHERE AceAssessmentId IN ('10', '11')
),

ge AS (
    SELECT
      AssessmentId,
      StudentResult AS GradeEquivalentScore
    FROM star_assessments
    WHERE ReportingMethod = 'Grade Equivalent'
),

gp AS (
    SELECT
      AssessmentId,
      CAST(StudentResult AS FLOAT64) AS GradePlacement
    FROM star_assessments
    WHERE ReportingMethod = 'Grade Placement'
),

ge_minus_gp AS (
    SELECT
      ge.AssessmentId,
      CASE
        WHEN ge.GradeEquivalentScore = '< 1' THEN 0.9 - gp.GradePlacement
        WHEN ge.GradeEquivalentScore = '> 9' THEN 9.1 - gp.GradePlacement
        WHEN ge.GradeEquivalentScore = '> 10' THEN 10.1 - gp.GradePlacement
        WHEN ge.GradeEquivalentScore = '> 11' THEN 11.1 - gp.GradePlacement
        WHEN ge.GradeEquivalentScore = '> 12' THEN 12.1 - gp.GradePlacement
        WHEN ge.GradeEquivalentScore = '> 12.9' THEN 11.1 - gp.GradePlacement
        ELSE CAST(ge.GradeEquivalentScore AS FLOAT64) - gp.GradePlacement
      END AS GeScoreMinusGp
    FROM ge
    LEFT JOIN gp
    USING (AssessmentId)
)

SELECT
  a.AssessmentId,
  ge.GradeEquivalentScore,
  gp.GradePlacement,
  ROUND(ge_minus_gp.GeScoreMinusGp, 2) AS GeMinusGp,
  CASE
    WHEN ge_minus_gp.GeScoreMinusGp > 0 THEN 'Above GP'
    WHEN ge_minus_gp.GeScoreMinusGp = 0 THEN 'At GP'
    WHEN ge_minus_gp.GeScoreMinusGp < 0 THEN 'Below GP'
  END AS GeScoreRelativeToGpCategory
FROM star_assessments AS a
LEFT JOIN ge
ON a.AssessmentId = ge.AssessmentId
LEFT JOIN gp
ON a.AssessmentId = gp.AssessmentId
LEFT JOIN ge_minus_gp
ON a.AssessmentId = ge_minus_gp.AssessmentId