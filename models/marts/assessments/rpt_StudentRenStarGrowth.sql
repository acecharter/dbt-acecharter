SELECT
  * EXCEPT (StudentResultDataType, StudentResult),
  CAST(StudentResult AS INT) AS StudentResult,
  CASE
    WHEN CAST(StudentResult AS INT) >= 66 THEN 'High Growth'
    WHEN
      CAST(StudentResult AS INT) >= 35 AND
      CAST(StudentResult AS INT) <= 65
    THEN 'Average Growth'
    WHEN CAST(StudentResult AS INT) <= 34 THEN 'Low Growth'
  END AS GrowthLevel,
  CASE
    WHEN CAST(StudentResult AS INT) >= 35 THEN 'Yes'
    ELSE 'No' 
  END AS AtOrAboveAverageGrowth,
  CASE
    WHEN
      AssessmentDate >= '2021-08-01' AND
      AssessmentDate <= '2021-08-11'
    THEN 'Fall (early)'
    WHEN
      AssessmentDate >= '2021-08-11' AND
      AssessmentDate <= '2021-09-30'
    THEN 'Fall'
    WHEN
      AssessmentDate >= '2021-10-01' AND
      AssessmentDate <= '2021-11-30'
    THEN 'Fall (late)'
    WHEN
      AssessmentDate >= '2021-12-01' AND
      AssessmentDate <= '2022-01-14'
    THEN 'Winter'
    WHEN
      AssessmentDate >= '2022-01-15' AND
      AssessmentDate <= '2022-03-31'
    THEN 'Winter (late)'
    WHEN
      AssessmentDate >= '2022-04-01' AND
      AssessmentDate <= '2022-04-14'
    THEN 'Spring (early)'
    WHEN
      AssessmentDate >= '2022-04-15' AND
      AssessmentDate <= '2022-05-31'
    THEN 'Spring'
    WHEN
      AssessmentDate >= '2022-06-01' AND
      AssessmentDate <= '2022-07-31'
    THEN 'Spring (late)'
  END AS TestingWindow
FROM {{ ref('rpt_StudentAssessment')}}
WHERE
  AssessmentName IN ('Star Reading', 'Star Math')
  AND ReportingMethod LIKE 'SGP%'
  AND ReportingMethod != 'SGP (current)'