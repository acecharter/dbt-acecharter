WITH
  completion_by_window AS (
    SELECT * FROM {{ref('fct_StudentRenStarCompletionByWindow')}}
  ),

  student_testing AS (
    SELECT DISTINCT
      SchoolYear,
      SchoolId,
      StudentUniqueId,
      AceAssessmentId,
    FROM completion_by_window
  ),

  fall AS (
    SELECT *
    FROM completion_by_window
    WHERE StarTestingWindow = 'Fall'
  ),

  winter AS (
    SELECT *
    FROM completion_by_window
    WHERE StarTestingWindow = 'Winter'
  ),
  
  spring AS (
    SELECT *
    FROM completion_by_window
    WHERE StarTestingWindow = 'Spring'
  ),

  fall_spring AS (
    SELECT
      t.*,
      'Fall to Spring' AS TestingPeriod,
      f.TestingStatus AS PreTestStatus,
      s.TestingStatus AS PostTestStatus,
      f.TestingRequiredBasedOnEnrollmentDates AS PreTestRequired,
      s.TestingRequiredBasedOnEnrollmentDates AS PostTestRequired
    FROM student_testing AS t
    LEFT JOIN fall AS f
    ON
      t.SchoolYear = f.SchoolYear
      AND t.SchoolId = f.SchoolId
      AND t.StudentUniqueId = f.StudentUniqueId
      AND t.AceAssessmentId = f.AceAssessmentId
    LEFT JOIN spring AS s
    ON
      t.SchoolYear = s.SchoolYear
      AND t.SchoolId = s.SchoolId
      AND t.StudentUniqueId = s.StudentUniqueId
      AND t.AceAssessmentId = s.AceAssessmentId
  ),

  fall_winter AS (
    SELECT
      t.*,
      'Fall to Winter' AS TestingPeriod,
      f.TestingStatus AS PreTestStatus,
      w.TestingStatus AS PostTestStatus,
      f.TestingRequiredBasedOnEnrollmentDates AS PreTestRequired,
      w.TestingRequiredBasedOnEnrollmentDates AS PostTestRequired
    FROM student_testing AS t
    LEFT JOIN fall AS f
    ON
      t.SchoolYear = f.SchoolYear
      AND t.SchoolId = f.SchoolId
      AND t.StudentUniqueId = f.StudentUniqueId
      AND t.AceAssessmentId = f.AceAssessmentId
    LEFT JOIN winter AS w
    ON
      t.SchoolYear = w.SchoolYear
      AND t.SchoolId = w.SchoolId
      AND t.StudentUniqueId = w.StudentUniqueId
      AND t.AceAssessmentId = w.AceAssessmentId
  ),

  winter_spring AS (
    SELECT
      t.*,
      'Winter to Spring' AS TestingPeriod,
      w.TestingStatus AS PreTestStatus,
      s.TestingStatus AS PostTestStatus,
      w.TestingRequiredBasedOnEnrollmentDates AS PreTestRequired,
      s.TestingRequiredBasedOnEnrollmentDates AS PostTestRequired
    FROM student_testing AS t
    LEFT JOIN winter AS w
    ON
      t.SchoolYear = w.SchoolYear
      AND t.SchoolId = w.SchoolId
      AND t.StudentUniqueId = w.StudentUniqueId
      AND t.AceAssessmentId = w.AceAssessmentId
    LEFT JOIN spring AS s
    ON
      t.SchoolYear = s.SchoolYear
      AND t.SchoolId = s.SchoolId
      AND t.StudentUniqueId = s.StudentUniqueId
      AND t.AceAssessmentId = s.AceAssessmentId
  ),

  unioned AS (
    SELECT * FROM fall_spring
    UNION ALL
    SELECT * FROM fall_winter
    UNION ALL
    SELECT * FROM winter_spring
  ),

  final AS (
    SELECT
      *,
      CASE WHEN PreTestRequired = 'Yes' AND PostTestRequired = 'Yes' THEN 'Yes' ELSE 'No' END AS PreAndPostTestRequired,
      CASE WHEN PreTestStatus = 'Tested' AND PostTestStatus = 'Tested' THEN 'Yes' ELSE 'No' END AS CompletedPreAndPostTest,
      CASE
        WHEN
          (PreTestRequired = 'Yes' OR PreTestStatus = 'Tested')
          AND (PostTestRequired = 'Yes' OR PostTestStatus = 'Tested')
        THEN 'Yes' ELSE 'No'
      END AS IncludeInPeriodCompletionRate,
    FROM unioned
  )


SELECT * FROM final