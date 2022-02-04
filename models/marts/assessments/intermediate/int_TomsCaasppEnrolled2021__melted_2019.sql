WITH caaspp AS (
  SELECT
    AceAssessmentId,
    AssessmentName,
    RecordType,
    StateUniqueId,
    CAST(NULL AS STRING) AS TestedSchoolId,
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    '2018-19' AS SchoolYear,
    GradeAssessedMinus2 AS GradeAssessed,
    AchievementLevelMinus2 AS AchievementLevel,
    DistanceFromStandardMinus2 AS DistanceFromStandard,
    ScaleScoreMinus2 AS ScaleScore
  FROM {{ ref('stg_RawData__TomsCaasppEnrolled2021') }}
  WHERE GradeAssessedMinus2 IS NOT NULL
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
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    ScaleScore AS StudentResult
  FROM caaspp
),

achievement_level AS (
  SELECT
    AssessmentId,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    AchievementLevel AS StudentResult
  FROM caaspp
),

dfs AS(
  SELECT
    AssessmentId,
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
 
