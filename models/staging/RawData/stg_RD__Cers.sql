{{ config(
    materialized='table'
)}}

WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      CASE
        WHEN AssessmentNameShort = 'SB ELA Summative' THEN 'ELA SUM'
        WHEN AssessmentNameShort = 'SB Math Summative' THEN 'Math SUM'
        WHEN AssessmentNameShort = 'CAA ELA' THEN 'CAAELA SUM'
        WHEN AssessmentNameShort = 'CAA Math' THEN 'CAAMATH SUM'
        WHEN AssessmentNameShort = 'CAST' THEN 'CAST SUM'
        WHEN AssessmentNameShort = 'CSA' THEN 'CSA SUM'
        WHEN AssessmentNameShort = 'SB ELA IAB/FIAB' THEN 'ELA IAB'
        WHEN AssessmentNameShort = 'SB Math IAB/FIAB' THEN 'Math IAB'
        WHEN AssessmentNameShort = 'SB ELA ICA' THEN 'ELA ICA'
        WHEN AssessmentNameShort = 'SB Math ICA' THEN 'Math ICA'
        WHEN AssessmentNameShort = 'Summative ELPAC' THEN 'ELPAC SUM'
        WHEN AssessmentNameShort = 'ALT ELPAC' THEN 'ALTELPAC SUM'
      END AS SubjectAssessmentSubType
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE 
      SystemOrVendorName = 'CAASPP' 
      OR SystemOrVendorName = 'ELPAC'
  ),

  cers_1819 AS(
    SELECT * FROM {{ ref('base_RD__Cers1819') }}
  ),

  cers_1920 AS(
    SELECT * FROM {{ ref('base_RD__Cers1920') }}
  ),

  cers_2021 AS(
    SELECT * FROM {{ ref('base_RD__Cers2021') }}
  ),

  cers_2122 AS(
    SELECT * FROM {{ ref('base_RD__Cers2122') }}
  ),

  cers_2223 AS(
    SELECT * FROM {{ ref('base_RD__Cers2223') }}
  ),

  unioned AS (
    SELECT * FROM cers_1819 
    UNION ALL
    SELECT * FROM cers_1920
    UNION ALL
    SELECT * FROM cers_2021
    UNION ALL
    SELECT * FROM cers_2122
    UNION ALL
    SELECT * FROM cers_2223
       
  ),

  final AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      u.DistrictId AS TestDistrictId,
      u.DistrictName AS TestDistrictName,
      u.SchoolId AS TestSchoolCdsCode,
      u.SchoolName AS TestSchoolName,
      u.StudentIdentifier AS StateUniqueId,
      u.FirstName,
      u.LastOrSurname AS LastSurname,
      DATE(u.SubmitDateTime) AS AssessmentDate,
      CAST(u.SchoolYear AS INT64) AS TestSchoolYear,
      u.TestSessionId,
      u.AssessmentType,
      u.AssessmentSubType,
      u.AssessmentName,
      u.Subject,
      CASE
        WHEN u.GradeLevelWhenAssessed = 'IT' THEN -4
        WHEN u.GradeLevelWhenAssessed = 'PR' THEN -3
        WHEN u.GradeLevelWhenAssessed = 'PK' THEN -2
        WHEN u.GradeLevelWhenAssessed = 'TK' THEN -1
        WHEN u.GradeLevelWhenAssessed = 'KG' THEN 0
        WHEN u.GradeLevelWhenAssessed = 'PS' THEN 14
        WHEN u.GradeLevelWhenAssessed = 'UG' THEN CAST(NULL AS INT64)
        ELSE CAST(u.GradeLevelWhenAssessed AS INT64)
      END AS GradeLevelWhenAssessed,
      u.Completeness,
      u.AdministrationCondition,
      SAFE_CAST(u.ScaleScoreAchievementLevel AS INT64) AS ScaleScoreAchievementLevel,
      SAFE_CAST(u.ScaleScore AS INT64) AS ScaleScore,
      NULLIF(u.Alt1ScoreAchievementLevel,'') AS Alt1ScoreAchievementLevel,
      NULLIF(u.Alt2ScoreAchievementLevel,'') AS Alt2ScoreAchievementLevel,
      NULLIF(u.Claim1ScoreAchievementLevel,'') AS Claim1ScoreAchievementLevel,
      NULLIF(u.Claim2ScoreAchievementLevel,'') AS Claim2ScoreAchievementLevel,
      NULLIF(u.Claim3ScoreAchievementLevel,'') AS Claim3ScoreAchievementLevel,
      NULLIF(u.Claim4ScoreAchievementLevel,'') AS Claim4ScoreAchievementLevel
    FROM unioned AS u
    LEFT JOIN assessment_ids AS a
    ON CONCAT(u.Subject, ' ', u.AssessmentSubType) = a.SubjectAssessmentSubType
  )

SELECT DISTINCT * FROM final