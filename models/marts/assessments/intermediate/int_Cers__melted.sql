{{ config(
    materialized='table'
)}}

WITH
  cers AS (
    SELECT
      *,
      CONCAT(AssessmentName, '-', StateUniqueId, '-', AssessmentDate) AS AssessmentId,
    FROM {{ ref('stg_RD__Cers') }}
    WHERE Completeness='Complete'
  ),

  cers_keys AS(
    SELECT
      AssessmentId,
      AceAssessmentId,
      AceAssessmentName AS AssessmentName,
      TestSchoolCdsCode AS TestedSchoolId,
      StateUniqueId,
      AssessmentDate,
      CONCAT(
        CAST(CAST(TestSchoolYear AS INT64) - 1 AS STRING),
        "-",
        RIGHT(CAST(TestSchoolYear AS STRING), 2)
      ) AS SchoolYear,
      AssessmentName AS GradeAssessmentName,
      AssessedGradeLevel,
      AdministrationCondition
    FROM cers
  ),

  achievement_level AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(ScaleScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE ScaleScoreAchievementLevel IS NOT NULL
  ),

  scale_score AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Scale Score' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(ScaleScore AS STRING) AS StudentResult
    FROM cers
    WHERE ScaleScore IS NOT NULL
  ),

  alt1_level AS (
    SELECT
      AssessmentId,
      'Oral Language' AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Alt1ScoreAchievementLevel IS NOT NULL
  ),

  alt2_level AS (
    SELECT
      AssessmentId,
      'Written Language' AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Alt2ScoreAchievementLevel IS NOT NULL
  ),

  claim1_level AS (
    SELECT
      AssessmentId,
      CASE
        WHEN Subject = 'ELPAC' THEN 'Listening'
        WHEN Subject='CAST' THEN 'Life Sciences'
        WHEN Subject = 'ELA' THEN 'Reading'
        WHEN Subject = 'Math' THEN 'Concepts and Procedures'
      END AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Claim1ScoreAchievementLevel IS NOT NULL
  ),

  claim2_level AS (
    SELECT
      AssessmentId,
      CASE
        WHEN Subject = 'ELPAC' THEN 'Speaking'
        WHEN Subject='CAST' THEN 'Physical Sciences'
        WHEN Subject = 'ELA' THEN 'Writing'
        WHEN Subject = 'Math' THEN 'Problem Solving and Modeling & Data Analysis'
      END AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Claim2ScoreAchievementLevel IS NOT NULL
  ),

  claim3_level AS (
    SELECT
      AssessmentId,
      CASE
        WHEN Subject = 'ELPAC' THEN 'Reading'
        WHEN Subject='CAST' THEN 'Earth and Space Sciences'
        WHEN Subject = 'ELA' THEN 'Listening'
        WHEN Subject = 'Math' THEN 'Communicating Reasoning'
      END AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Claim3ScoreAchievementLevel IS NOT NULL
  ),

  claim4_level AS (
    SELECT
      AssessmentId,
      CASE
        WHEN Subject = 'ELPAC' THEN 'Writing'
        WHEN Subject = 'ELA' THEN 'Research and Inquiry'
      END AS AssessmentObjective,
      'Achievement Level' AS ReportingMethod,
      'INT64' AS StudentResultDataType,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS StudentResult
    FROM cers
    WHERE Claim4ScoreAchievementLevel IS NOT NULL
  ),

results_unioned AS(
  SELECT * FROM achievement_level
  UNION ALL
  SELECT * FROM scale_score
  UNION ALL
  SELECT * FROM alt1_level
  UNION ALL
  SELECT * FROM alt2_level
  UNION ALL
  SELECT * FROM claim1_level
  UNION ALL
  SELECT * FROM claim2_level
  UNION ALL
  SELECT * FROM claim3_level
  UNION ALL
  SELECT * FROM claim4_level
)

SELECT
 k.*,
 r.* EXCEPT (AssessmentID)
FROM cers_keys AS k
LEFT JOIN results_unioned AS r
USING (AssessmentId)
WHERE StudentResult IS NOT NULL