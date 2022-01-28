WITH caaspp AS (
  SELECT * FROM {{ ref('stg_RawData__TomsCaasppEnrolled2021') }}
),

caaspp_keys_2021 AS(
  SELECT
    --ASSESSMENT_KEY,
    --OBJECTIVE_ASSESSMENT_KEY,
    SSID,
    SchoolId,
    '2020-21' AS SchoolYear,
    CONCAT('2021-', RecordType, '-', SSID) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    GradeAssessed AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2021 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'Scale Score' AS StudentResultDataType,
    CAST(ScaleScore AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2021 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'Level' AS StudentResultDataType,
    CAST(AchievementLevels AS STRING) AS StudentResult
  FROM caaspp
),

caaspp_keys_2020 AS(
  SELECT
    --ASSESSMENT_KEY,
    --OBJECTIVE_ASSESSMENT_KEY,
    SSID,
    SchoolId,
    '2019-20' AS SchoolYear,
    CONCAT('2020-', RecordType, '-', SSID) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    GradeAssessedMinus1 AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2020 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'Scale Score' AS StudentResultDataType,
    CAST(ScaleScoreMinus1 AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2020 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'Level' AS StudentResultDataType,
    CAST(AchievementLevelMinus1 AS STRING) AS StudentResult
  FROM caaspp
),

caaspp_keys_2019 AS(
  SELECT
    --ASSESSMENT_KEY,
    --OBJECTIVE_ASSESSMENT_KEY,
    SSID,
    SchoolId,
    '2018-19' AS SchoolYear,
    CONCAT('2019-', RecordType, '-', SSID) AS AssessmentId,
    --FinalTestCompletedDate AS AssessmentDate,
    GradeAssessedMinus1 AS AssessedGradeLevel
  FROM caaspp
),

scale_score_2019 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Scale Score' AS ReportingMethod,
    'Scale Score' AS StudentResultDataType,
    CAST(ScaleScoreMinus2 AS STRING) AS StudentResult
  FROM caaspp
),

achievement_level_2019 AS (
  SELECT
    CONCAT(RecordType, "-", SSID) AS AssessmentId,
    RecordType,
    'Achievement Level' AS ReportingMethod,
    'Level' AS StudentResultDataType,
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

final AS(
  SELECT * FROM data_2021
  UNION ALL
  SELECT * FROM data_2020
  UNION ALL
  SELECT * FROM data_2019
)


SELECT *
FROM final
WHERE StudentResult IS NOT NULL