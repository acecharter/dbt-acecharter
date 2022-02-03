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
  FROM {{ ref('stg_RawData__TomsCaasppEnrolled2021') }}
  WHERE GradeAssessed IS NOT NULL
),

caaspp_keys AS(
  SELECT
    AssessmentId,
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear,
    CAST(GradeAssessed AS STRING) AS AssessedGradeLevel
  FROM caaspp
),

scale_score AS (
  SELECT
    AssessmentId,
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(ScaleScore AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level AS (
  SELECT
    AssessmentId,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(AchievementLevel AS STRING) AS StudentResult
  FROM caaspp
),

dfs AS(
  SELECT
    AssessmentId,
    'Distance From Standard' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(DistanceFromStandard AS STRING) AS StudentResult
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
 
