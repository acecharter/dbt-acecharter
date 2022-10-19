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
      u.DistrictId,
      u.TestDistrictName,
      u.TestSchoolCdsCode,
      u.TestSchoolName,
      u.StateUniqueId,
      u.FirstName,
      u.LastSurname,
      u.AssessmentDate,
      u.TestSchoolYear,
      u.TestSessionId,
      u.AssessmentType,
      u.AssessmentSubType,
      u.AssessmentName,
      u.Subject,
      u.GradeLevelWhenAssessed,
      u.Completeness,
      u.AdministrationCondition,
      u.ScaleScoreAchievementLevel,
      u.ScaleScore,
      u.Alt1ScoreAchievementLevel,
      u.Alt2ScoreAchievementLevel,
      u.Claim1ScoreAchievementLevel,
      u.Claim2ScoreAchievementLevel,
      u.Claim3ScoreAchievementLevel,
      u.Claim4ScoreAchievementLevel
    FROM unioned AS u
    LEFT JOIN assessment_ids AS a
    ON CONCAT(u.Subject, ' ', u.AssessmentSubType) = a.SubjectAssessmentSubType
  )

SELECT * FROM final