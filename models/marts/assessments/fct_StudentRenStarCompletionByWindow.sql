WITH 
  eligible AS (
    SELECT
      SchoolYear,
      SchoolId,
      StudentUniqueId,
      AceAssessmentId,
      TestingWindow AS StarTestingWindow
    FROM {{ ref('dim_RenStarWindowEligibleStudents') }}
  ),

  tested AS (
    SELECT *
    FROM {{ref('dim_RenStarWindowTestedStudents')}}
    WHERE AceAssessmentId NOT IN ('12', '13')
  ),

  joined AS (
    SELECT
      CASE WHEN e.SchoolYear IS NOT NULL THEN e.SchoolYear ELSE t.SchoolYear END AS SchoolYear,
      CASE WHEN e.SchoolId IS NOT NULL THEN e.SchoolId ELSE t.SchoolId END AS SchoolId,
      CASE WHEN e.StudentUniqueId IS NOT NULL THEN e.StudentUniqueId ELSE t.StudentUniqueId END AS StudentUniqueId,
      CASE WHEN e.AceAssessmentId IS NOT NULL THEN e.AceAssessmentId ELSE t.AceAssessmentId END AS AceAssessmentId,
      CASE WHEN e.StarTestingWindow IS NOT NULL THEN e.StarTestingWindow ELSE t.StarTestingWindow END AS StarTestingWindow,
      CASE WHEN t.SchoolYear IS NOT NULL THEN 'Tested' ELSE 'Not Tested' END AS TestingStatus,
      CASE WHEN e.SchoolYear IS NOT NULL THEN 'Yes' ELSE 'No' END AS TestingRequiredBasedOnEnrollmentDates
    FROM eligible AS e
    FULL OUTER JOIN tested AS t
    ON
      e.SchoolYear = t.SchoolYear
      AND e.SchoolId = t.SchoolId
      AND e.StudentUniqueId = t.StudentUniqueId
      AND e.AceAssessmentId = t.AceAssessmentId
      AND e.StarTestingWindow = t.StarTestingWindow
  )
  
SELECT * FROM joined


