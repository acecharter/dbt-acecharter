SELECT
  * EXCEPT (StudentResultDataType, StudentResult),
  CAST(StudentResult AS INT) AS StudentResult,
  CASE
    WHEN CAST(StudentResult AS INT) > 65 THEN 'High Growth'
    WHEN
      CAST(StudentResult AS INT) >= 50 AND
      CAST(StudentResult AS INT) <= 65
    THEN 'Above Average Growth'
    WHEN
      CAST(StudentResult AS INT) >= 35 AND
      CAST(StudentResult AS INT) < 50
    THEN 'Below Average Growth'
    WHEN CAST(StudentResult AS INT) < 35 THEN 'Low Growth'
  END AS GrowthLevel,
  CASE
    WHEN CAST(StudentResult AS INT) >= 35 THEN 'Yes'
    ELSE 'No' 
  END AS AtOrAboveAverageGrowth
FROM {{ ref('rpt_StudentAssessment')}}
WHERE
  AssessmentName IN ('Star Reading', 'Star Math')
  AND ReportingMethod LIKE 'SGP%'
  AND ReportingMethod != 'SGP (current)'