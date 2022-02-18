{{ config(
    materialized='table'
)}}

WITH elpac AS (
  SELECT
    AceAssessmentId,
    RecordType,
    StateUniqueId,
    TestedSchoolId,
    '2020-21' AS SchoolYear,
    CONCAT('2021-', AceAssessmentId, '-', StateUniqueId) AS AssessmentId,
    GradeAssessed AS AssessedGradeLevel,
    OverallScaleScore,
    OverallPL,
    ElpiLevel
  FROM {{ ref('stg_RD__TomsElpacEnrolled2021') }}
),

elpac_keys AS(
  SELECT
    AceAssessmentId,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear,
    AssessmentId,
    AssessedGradeLevel
  FROM elpac
),

scale_score AS (
  SELECT
    AssessmentId,
    RecordType,
    'Overall Scale Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallScaleScore AS StudentResult
  FROM elpac
),

performance_level AS (
  SELECT
    AssessmentId,
    RecordType,
    'Overall Performance Level' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    OverallPL AS StudentResult
  FROM elpac
),

elpi_level AS (
  SELECT
    AssessmentId,
    RecordType,
    'ELPI Level' AS ReportingMethod,
    'STRING' AS StudentResultDataType,
    ElpiLevel AS StudentResult
  FROM elpac
),

unioned_merged AS (
  SELECT 
    k.*,
    s.* EXCEPT(AssessmentId)
  FROM elpac_keys AS k
  LEFT JOIN scale_score AS s
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    p.* EXCEPT(AssessmentId)
  FROM elpac_keys AS k
  LEFT JOIN performance_level AS p
  USING (AssessmentId)

  UNION ALL
  SELECT 
    k.*,
    a.* EXCEPT(AssessmentId)
  FROM elpac_keys AS k
  LEFT JOIN elpi_level AS a
  USING (AssessmentId)
)

SELECT *
FROM unioned_merged
WHERE StudentResult IS NOT NULL