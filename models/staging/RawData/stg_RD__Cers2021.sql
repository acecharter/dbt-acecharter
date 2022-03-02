WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      CASE
        WHEN AssessmentNameShort = 'SB ELA' THEN 'ELA SUM'
        WHEN AssessmentNameShort = 'SB Math' THEN 'Math SUM'
        WHEN AssessmentNameShort = 'CAA ELA' THEN 'CAAELA SUM'
        WHEN AssessmentNameShort = 'CAA Math' THEN 'CAAMATH SUM'
        WHEN AssessmentNameShort = 'CAST' THEN 'CAST SUM'
        WHEN AssessmentNameShort = 'CSA' THEN 'CSA SUM'
        WHEN AssessmentNameShort = 'SB ELA IAB' THEN 'ELA IAB'
        WHEN AssessmentNameShort = 'SB Math IAB' THEN 'Math IAB'
        WHEN AssessmentNameShort = 'SB ELA ICA' THEN 'ELA ICA'
        WHEN AssessmentNameShort = 'SB Math ICA' THEN 'Math ICA'
        WHEN AssessmentNameShort = 'Summative ELPAC' THEN 'ELPAC SUM'
      END AS SubjectAssessmentSubType
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE 
      SystemOrVendorName = 'CAASPP' 
      OR SystemOrVendorName = 'ELPAC'
  ),

  empower AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS AssessmentDate,
      SchoolYear AS TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      CAST(GradeLevelWhenAssessed AS STRING) AS AssessedGradeLevel,
      Completeness,
      AdministrationCondition,
      ScaleScoreAchievementLevel,
      ScaleScore,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel,
    FROM {{ source('RawData', 'CersEmpower2021')}}
  ),

  esperanza AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS AssessmentDate,
      SchoolYear AS TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      CAST(GradeLevelWhenAssessed AS STRING) AS AssessedGradeLevel,
      Completeness,
      AdministrationCondition,
      ScaleScoreAchievementLevel,
      ScaleScore,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel
    FROM {{ source('RawData', 'CersEsperanza2021')}}
  ),

  inspire AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS AssessmentDate,
      SchoolYear AS TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      CAST(GradeLevelWhenAssessed AS STRING) AS AssessedGradeLevel,
      Completeness,
      AdministrationCondition,
      ScaleScoreAchievementLevel,
      ScaleScore,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel
    FROM {{ source('RawData', 'CersInspire2021')}}
  ),

  hs AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS AssessmentDate,
      SchoolYear AS TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      CAST(GradeLevelWhenAssessed AS STRING) AS AssessedGradeLevel,
      Completeness,
      AdministrationCondition,
      ScaleScoreAchievementLevel,
      ScaleScore,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel
    FROM {{ source('RawData', 'CersHighSchool2021')}}
  ),

  cers_unioned AS (
    SELECT * FROM empower
    UNION ALL
    SELECT * FROM esperanza
    UNION ALL
    SELECT * FROM inspire
    UNION ALL
    SELECT * FROM hs
  ),

  final AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      c.*
    FROM cers_unioned AS c
    LEFT JOIN assessment_ids AS a
    ON CONCAT(c.Subject, ' ', c.AssessmentSubType) = a.SubjectAssessmentSubType
  )

SELECT DISTINCT * FROM final