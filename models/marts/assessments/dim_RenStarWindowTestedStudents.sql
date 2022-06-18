WITH
  tested_by_window AS (
    SELECT DISTINCT
      SchoolYear,
      TestedSchoolId,
      StudentIdentifier AS StudentUniqueId,
      AceAssessmentId,
      AssessmentName,
      StarTestingWindow
    FROM {{ref('int_RenStar__1_unioned')}}
    WHERE
      AssessmentType LIKE '%Enterprise%'
      AND StudentIdentifier IS NOT NULL
  ),

  tested AS (
    SELECT DISTINCT
      SchoolYear,
      TestedSchoolId,
      StudentUniqueId,
      AceAssessmentId,
      AssessmentName
    FROM tested_by_window
  ),

  fall AS (
    SELECT *
    FROM tested_by_window
    WHERE StarTestingWindow = 'Fall'
  ),

  winter AS (
    SELECT *
    FROM tested_by_window
    WHERE StarTestingWindow = 'Winter'
  ),
  
  spring AS (
    SELECT *
    FROM tested_by_window
    WHERE StarTestingWindow = 'Spring'
  ),

  final AS (
    SELECT
      t.*,
      CASE WHEN f.StarTestingWindow IS NOT NULL THEN 'Yes' ELSE 'No' END AS FallTest,
      CASE WHEN w.StarTestingWindow IS NOT NULL THEN 'Yes' ELSE 'No' END AS WinterTest,
      CASE WHEN s.StarTestingWindow IS NOT NULL THEN 'Yes' ELSE 'No' END AS SpringTest,
      CASE
        WHEN (
          (f.StarTestingWindow IS NOT NULL) 
          AND (s.StarTestingWindow IS NOT NULL)
        ) THEN 'Yes' ELSE 'No'
      END AS FallAndSpringTest,
      CASE
        WHEN (
          (f.StarTestingWindow IS NOT NULL) 
          AND (w.StarTestingWindow IS NOT NULL) 
          AND (s.StarTestingWindow IS NOT NULL)
        ) THEN 'Yes' ELSE 'No'
      END AS FallWinterAndSpringTest
    FROM tested AS t
    LEFT JOIN fall AS f
    ON
      t.SchoolYear = f.SchoolYear
      AND t.TestedSchoolId = f.TestedSchoolId
      AND t.StudentUniqueId = f.StudentUniqueId
      AND t.AceAssessmentId = f.AceAssessmentId
    LEFT JOIN winter AS w
    ON
      t.SchoolYear = w.SchoolYear
      AND t.TestedSchoolId = w.TestedSchoolId
      AND t.StudentUniqueId = w.StudentUniqueId
      AND t.AceAssessmentId = w.AceAssessmentId
    LEFT JOIN spring AS s
    ON
      t.SchoolYear = s.SchoolYear
      AND t.TestedSchoolId = s.TestedSchoolId
      AND t.StudentUniqueId = s.StudentUniqueId
      AND t.AceAssessmentId = s.AceAssessmentId
  )

SELECT * FROM final