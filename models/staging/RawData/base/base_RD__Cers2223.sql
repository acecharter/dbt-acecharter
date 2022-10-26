WITH
  empower AS (
    SELECT
      FORMAT("%014d", CAST(DistrictId AS INT64)) AS TestDistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", CAST(SchoolId AS INT64)) AS TestSchoolCdsCode,
      SchoolName AS TestSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS AssessmentDate,
      CAST(SchoolYear AS INT64) AS TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      CAST(GradeLevelWhenAssessed AS INT64) AS GradeLevelWhenAssessed,
      Completeness,
      AdministrationCondition,
      SAFE_CAST(ScaleScoreAchievementLevel AS INT64) AS ScaleScoreAchievementLevel,
      SAFE_CAST(ScaleScore AS INT64) AS ScaleScore,
      CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
      CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
      CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
      CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
      CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
      CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel
    FROM {{ source('RawData', 'CersEmpower2223')}}
  ),

  esperanza AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSchoolName,
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
      CAST(GradeLevelWhenAssessed AS INT64) AS GradeLevelWhenAssessed,
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
    FROM {{ source('RawData', 'CersEsperanza2223')}}
  ),

  inspire AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSchoolName,
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
      CAST(GradeLevelWhenAssessed AS INT64) AS GradeLevelWhenAssessed,
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
    FROM {{ source('RawData', 'CersInspire2223')}}
  ),

--   hs AS (
--     SELECT
--       FORMAT("%014d", DistrictId) AS DistrictId,
--       DistrictName AS TestDistrictName,
--       FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
--       SchoolName AS TestSchoolName,
--       CAST(StudentIdentifier AS STRING) AS StateUniqueId,
--       FirstName,
--       LastOrSurname AS LastSurname,
--       DATE(SubmitDateTime) AS AssessmentDate,
--       SchoolYear AS TestSchoolYear,
--       TestSessionId,
--       AssessmentType,
--       AssessmentSubType,
--       AssessmentName,
--       Subject,
--       CAST(GradeLevelWhenAssessed AS INT64) AS GradeLevelWhenAssessed,
--       Completeness,
--       AdministrationCondition,
--       ScaleScoreAchievementLevel,
--       ScaleScore,
--       CAST(Alt1ScoreAchievementLevel AS STRING) AS Alt1ScoreAchievementLevel,
--       CAST(Alt2ScoreAchievementLevel AS STRING) AS Alt2ScoreAchievementLevel,
--       CAST(Claim1ScoreAchievementLevel AS STRING) AS Claim1ScoreAchievementLevel,
--       CAST(Claim2ScoreAchievementLevel AS STRING) AS Claim2ScoreAchievementLevel,
--       CAST(Claim3ScoreAchievementLevel AS STRING) AS Claim3ScoreAchievementLevel,
--       CAST(Claim4ScoreAchievementLevel AS STRING) AS Claim4ScoreAchievementLevel
--     FROM {{ source('RawData', 'CersHighSchool2223')}}
--   ),

  cers_unioned AS (
    SELECT * FROM empower
    UNION ALL
    SELECT * FROM esperanza
    UNION ALL
    SELECT * FROM inspire
    -- UNION ALL
    -- SELECT * FROM hs
  ),

  final AS (
    SELECT
      * EXCEPT (
        Alt1ScoreAchievementLevel,
        Alt2ScoreAchievementLevel,
        Claim1ScoreAchievementLevel,
        Claim2ScoreAchievementLevel,
        Claim3ScoreAchievementLevel,
        Claim4ScoreAchievementLevel
      ),
      NULLIF(Alt1ScoreAchievementLevel,'') AS Alt1ScoreAchievementLevel,
      NULLIF(Alt2ScoreAchievementLevel,'') AS Alt2ScoreAchievementLevel,
      NULLIF(Claim1ScoreAchievementLevel,'') AS Claim1ScoreAchievementLevel,
      NULLIF(Claim2ScoreAchievementLevel,'') AS Claim2ScoreAchievementLevel,
      NULLIF(Claim3ScoreAchievementLevel,'') AS Claim3ScoreAchievementLevel,
      NULLIF(Claim4ScoreAchievementLevel,'') AS Claim4ScoreAchievementLevel
    FROM cers_unioned
  )

SELECT DISTINCT * FROM final