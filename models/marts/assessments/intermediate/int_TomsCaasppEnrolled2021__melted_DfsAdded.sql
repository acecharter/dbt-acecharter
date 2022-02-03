WITH caaspp AS (
  SELECT * FROM {{ ref('stg_RawData__TomsCaasppEnrolled2021') }}
),

caaspp_keys_2021 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    '2020-21' AS SchoolYear,
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    CAST(GradeAssessed AS STRING) AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2021 AS (
  SELECT
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(ScaleScore AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2021 AS (
  SELECT
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(AchievementLevels AS STRING) AS StudentResult
  FROM caaspp
),

caaspp_keys_2020 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    '2019-20' AS SchoolYear,
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    CAST(GradeAssessedMinus1 AS STRING) AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2020 AS (
  SELECT
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(ScaleScoreMinus1 AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2020 AS (
  SELECT
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(AchievementLevelMinus1 AS STRING) AS StudentResult
  FROM caaspp
),

caaspp_keys_2019 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    SchoolId,
    '2018-19' AS SchoolYear,
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    CAST(GradeAssessedMinus2 AS STRING) AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2019 AS (
  SELECT
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(ScaleScoreMinus2 AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2019 AS (
  SELECT
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(AchievementLevelMinus2 AS STRING) AS StudentResult
  FROM caaspp
),

data_2021 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2021 AS k
  LEFT JOIN scale_score_2021 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2021 AS k
  LEFT JOIN achievement_level_2021 AS a
  USING (AssessmentId)
),

data_2020 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2020 AS k
  LEFT JOIN scale_score_2020 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2020 AS k
  LEFT JOIN achievement_level_2020 AS a
  USING (AssessmentId)
),

data_2019 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2019 AS k
  LEFT JOIN scale_score_2019 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM caaspp_keys_2019 AS k
  LEFT JOIN achievement_level_2019 AS a
  USING (AssessmentId)
),

unioned AS(
  SELECT * FROM data_2021
  UNION ALL
  SELECT * FROM data_2020
  UNION ALL
  SELECT * FROM data_2019
  WHERE StudentResult IS NOT NULL
),

min_met_scores AS (
  SELECT
    AceAssessmentId,
    GradeLevel,
    MinStandardMetScaleScore
  FROM {{ ref('fct_CaasppMinMetScaleScores') }}
  WHERE Area='Overall'
),

dfs_results AS(
  SELECT
    u.* EXCEPT(ReportingMethod, StudentResult),
    'Distance From Standard' AS ReportingMethod,
    (u.StudentResult - m.MinStandardMetScaleScore) AS StudentResult
  FROM unioned AS u
  LEFT JOIN min_met_scores AS m
  ON
    u.AceAssessmentId = m.AceAssessmentId AND
    u.AssessedGradeLevel = m.GradeLevel
  WHERE u.ReportingMethod = 'Scale Score'
),

final AS (
  SELECT * FROM unioned
  UNION ALL
  SELECT * FROM dfs_results
)

SELECT * FROM final
 
