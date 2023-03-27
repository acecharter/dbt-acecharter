WITH
  star_assessments AS (
    SELECT
      AceAssessmentId,
      AssessmentNameShort AS AssessmentName,
      AssessmentSubject
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE AssessmentFamilyNameShort = 'Star'
  ), 

  eligible AS (
    SELECT
      SchoolYear,
      SchoolId,
      StudentUniqueId,
      TestingWindow AS StarTestingWindow
    FROM {{ ref('dim_RenStarWindowEligibleStudents') }}
  ),

  eligible_by_star_assessment AS (
    SELECT
      e.*,
      s.*
    FROM eligible AS e
    CROSS JOIN star_assessments AS s
  ),

  result_counts AS (
    SELECT * FROM {{ref('fct_StudentRenStarWindowResultCounts')}}
  ),

  joined AS (
    SELECT
      CASE WHEN e.SchoolYear IS NOT NULL THEN e.SchoolYear ELSE r.SchoolYear END AS SchoolYear,
      CASE WHEN e.StarTestingWindow IS NOT NULL THEN e.StarTestingWindow ELSE r.StarTestingWindow END AS StarTestingWindow,
      CASE WHEN e.SchoolId IS NOT NULL THEN e.SchoolId ELSE r.SchoolId END AS SchoolId,
      CASE WHEN e.StudentUniqueId IS NOT NULL THEN e.StudentUniqueId ELSE r.StudentUniqueId END AS StudentUniqueId,
      CASE WHEN e.SchoolYear IS NOT NULL THEN 'Yes' ELSE 'No' END AS TestingRequiredBasedOnEnrollmentDates,
      CASE WHEN e.AceAssessmentId IS NOT NULL THEN e.AceAssessmentId ELSE r.AceAssessmentId END AS AceAssessmentId,
      CASE WHEN e.AssessmentName IS NOT NULL THEN e.AssessmentName ELSE r.AssessmentName END AS AssessmentName,
      CASE WHEN r.ResultCount IS NOT NULL THEN r.ResultCount ELSE 0 END AS ResultCount,
      CASE WHEN r.ResultCount > 0 THEN 'Tested' ELSE 'Not Tested' END AS TestingStatus
    FROM eligible_by_star_assessment AS e
    FULL OUTER JOIN result_counts AS r
    ON
      e.SchoolYear = r.SchoolYear
      AND e.StarTestingWindow = r.StarTestingWindow
      AND e.SchoolId = r.SchoolId
      AND e.StudentUniqueId = r.StudentUniqueId
      AND e.AceAssessmentId = r.AceAssessmentId
  ),

  math AS (
    SELECT * FROM joined
    WHERE AceAssessmentId ='10'
  ),

  reading AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '11'
  ),

  math_spanish AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '12'
  ),

  reading_spanish AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '13'
  ),

  early_literacy AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '21'
  ),

  early_literacy_spanish AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '22'
  ),

  math_progress_monitoring AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '23'
  ),

  reading_progress_monitoring AS (
    SELECT * FROM joined
    WHERE AceAssessmentId = '24'
  ),
  
  math_joined AS (
    SELECT
      m.* EXCEPT (AceAssessmentId, TestingStatus, ResultCount),
      m.AssessmentName AS AssessmentSubject,
      IFNULL(m.ResultCount, 0) + IFNULL(pm.ResultCount, 0) AS ResultCount,
      IFNULL(s.ResultCount, 0) AS ResultCountOther,
      CASE
        WHEN (IFNULL(m.ResultCount, 0) + IFNULL(pm.ResultCount, 0)) > 0 THEN 'Tested'
        WHEN IFNULL(s.ResultCount, 0) > 0 THEN 'Other Tested (Spanish)'
        ELSE 'Not Tested'
      END AS TestingStatus
    FROM math AS m
    LEFT JOIN math_progress_monitoring AS pm
    ON
      m.SchoolYear = pm.SchoolYear
      AND m.StarTestingWindow = pm.StarTestingWindow
      AND m.SchoolId = pm.SchoolId
      AND m.StudentUniqueId = pm.StudentUniqueId
    LEFT JOIN math_spanish AS s
    ON
      m.SchoolYear = s.SchoolYear
      AND m.StarTestingWindow = s.StarTestingWindow
      AND m.SchoolId = s.SchoolId
      AND m.StudentUniqueId = s.StudentUniqueId
  ),

  reading_joined AS (
    SELECT
      r.* EXCEPT (AceAssessmentId, TestingStatus, ResultCount),
      r.AssessmentName AS AssessmentSubject,
      IFNULL(r.ResultCount, 0) + IFNULL(pm.ResultCount, 0) AS ResultCount,
      IFNULL(el.ResultCount, 0) + IFNULL(els.ResultCount, 0) + IFNULL(s.ResultCount, 0) AS ResultCountOther,
      CASE
        WHEN (IFNULL(r.ResultCount, 0) + IFNULL(pm.ResultCount, 0)) > 0 THEN 'Tested'
        WHEN (IFNULL(el.ResultCount, 0) + IFNULL(els.ResultCount, 0) + IFNULL(s.ResultCount, 0)) > 0 THEN 'Other Tested (Early Literacy or Spanish)'
        ELSE 'Not Tested'
      END AS TestingStatus
    FROM reading AS r
    LEFT JOIN reading_progress_monitoring AS pm
    ON
      r.SchoolYear = pm.SchoolYear
      AND r.StarTestingWindow = pm.StarTestingWindow
      AND r.SchoolId = pm.SchoolId
      AND r.StudentUniqueId = pm.StudentUniqueId
    LEFT JOIN early_literacy AS el
    ON
      r.SchoolYear = el.SchoolYear
      AND r.StarTestingWindow = el.StarTestingWindow
      AND r.SchoolId = el.SchoolId
      AND r.StudentUniqueId = el.StudentUniqueId
    LEFT JOIN early_literacy_spanish AS els
    ON
      r.SchoolYear = els.SchoolYear
      AND r.StarTestingWindow = els.StarTestingWindow
      AND r.SchoolId = els.SchoolId
      AND r.StudentUniqueId = els.StudentUniqueId
    LEFT JOIN reading_spanish AS s
    ON
      r.SchoolYear = s.SchoolYear
      AND r.StarTestingWindow = s.StarTestingWindow
      AND r.SchoolId = s.SchoolId
      AND r.StudentUniqueId = s.StudentUniqueId
  ),

  final AS (
    SELECT * FROM math_joined
    UNION ALL
    SELECT * FROM reading_joined
  )
SELECT * FROM final