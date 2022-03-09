{{ config(
    materialized='table'
)}}

WITH caaspp AS (
  SELECT
    AceAssessmentId,
    AssessmentName,
    RecordType,
    StateUniqueId,
    TestedSchoolId,
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    '2020-21' AS SchoolYear,
    GradeAssessed,
    AchievementLevel,
    DistanceFromStandard,
    ScaleScore
  FROM {{ ref('stg_RD__TomsCaasppEnrolled2021') }}
  WHERE GradeAssessed IS NOT NULL
),

caaspp_keys AS(
  SELECT
    AssessmentId,
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear,
    GradeAssessed AS AssessedGradeLevel
  FROM caaspp
),

scale_score AS (
  SELECT
    AssessmentId,
    'Overall' AS AssessmentObjective,
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    ScaleScore AS StudentResult
  FROM caaspp
),

achievement_level AS (
  SELECT
    AssessmentId,
    'Overall' AS AssessmentObjective,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    AchievementLevel AS StudentResult
  FROM caaspp
),

dfs AS(
  SELECT
    AssessmentId,
    'Overall' AS AssessmentObjective,
    'Distance From Standard' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    DistanceFromStandard AS StudentResult
  FROM caaspp
),

unioned_merged AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM caaspp_keys AS k
  LEFT JOIN scale_score AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM caaspp_keys AS k
  LEFT JOIN achievement_level AS a
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    d.* EXCEPT(AssessmentId)
  FROM caaspp_keys AS k
  LEFT JOIN dfs AS d
  USING (AssessmentId)
)

SELECT * FROM unioned_merged
WHERE StudentResult IS NOT NULL
 
