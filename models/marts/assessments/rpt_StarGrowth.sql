SELECT
  * EXCEPT (StudentResultDataType, StudentResult),
  CAST(StudentResult AS INT64) AS StudentResult,
  CASE
    WHEN CAST(StudentResult AS INT64) >= 35 THEN 'Yes'
    ELSE 'No' 
  END AS AverageOrAboveGrowth
FROM {{ ref('rpt_StudentAssessment')}}
WHERE
  AssessmentName IN ('Star Reading', 'Star Math')
  AND ReportingMethod LIKE 'Student Growth Percentile%'