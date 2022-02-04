WITH elpac AS (
  SELECT * FROM {{ ref('stg_RawData__TomsElpacEnrolled2021') }}
),

elpac_keys_2021 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    '2020-21' AS SchoolYear,
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    GradeAssessed AS AssessedGradeLevel
  FROM elpac
),

scale_score_2021 AS (
  SELECT
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallScaleScore AS StudentResult
  FROM elpac
),

achievement_level_2021 AS (
  SELECT
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Performance Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallPL AS StudentResult
  FROM elpac
),

elpac_keys_2020 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    CAST(NULL AS STRING) AS TestedSchoolId,
    '2019-20' AS SchoolYear,
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    GradeAssessedMinus1 AS AssessedGradeLevel
  FROM elpac
),

scale_score_2020 AS (
  SELECT
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallScaleScoreMinus1 AS StudentResult
  FROM elpac
),

achievement_level_2020 AS (
  SELECT
    CONCAT('2020-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Performance Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallPLMinus1 AS StudentResult
  FROM elpac
),

elpac_keys_2019 AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    CAST(NULL AS STRING) AS TestedSchoolId,
    '2018-19' AS SchoolYear,
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    GradeAssessedMinus2 AS AssessedGradeLevel
  FROM elpac
),

scale_score_2019 AS (
  SELECT
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallScaleScoreMinus2 AS StudentResult
  FROM elpac
),

achievement_level_2019 AS (
  SELECT
    CONCAT('2019-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    RecordType,
    'Overall Performance Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallPLMinus2 AS StudentResult
  FROM elpac
),

data_2021 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM elpac_keys_2021 AS k
  LEFT JOIN scale_score_2021 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM elpac_keys_2021 AS k
  LEFT JOIN achievement_level_2021 AS a
  USING (AssessmentId)
),

data_2020 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM elpac_keys_2020 AS k
  LEFT JOIN scale_score_2020 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM elpac_keys_2020 AS k
  LEFT JOIN achievement_level_2020 AS a
  USING (AssessmentId)
),

data_2019 AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM elpac_keys_2019 AS k
  LEFT JOIN scale_score_2019 AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM elpac_keys_2019 AS k
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