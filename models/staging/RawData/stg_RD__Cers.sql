--HS file for 2122 is currently excluded since file was blank as of last update

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

  cers_empower_2122 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS TestDistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEmpower2122')}}
  ),

  cers_esperanza_2122 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEsperanza2122')}}
  ),

  cers_inspire_2122 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersInspire2122')}}
  ),

  cers_2122 AS(
    SELECT * FROM cers_empower_2122
    UNION ALL
    SELECT * FROM cers_esperanza_2122
    UNION ALL
    SELECT * FROM cers_inspire_2122
  ),

  cers_empower_2021 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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

  cers_esperanza_2021 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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

  cers_inspire_2021 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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

  cers_hs_2021 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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

  cers_2021 AS(
    SELECT * FROM cers_empower_2021
    UNION ALL
    SELECT * FROM cers_esperanza_2021
    UNION ALL
    SELECT * FROM cers_inspire_2021
    UNION ALL
    SELECT * FROM cers_hs_2021
  ),

  cers_empower_1920 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEmpower1920')}}
  ),

  cers_esperanza_1920 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEsperanza1920')}}
  ),

  cers_inspire_1920 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersInspire1920')}}
  ),

  cers_hs_1920 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersHighSchool1920')}}
  ),

  cers_1920 AS(
    SELECT * FROM cers_empower_1920
    UNION ALL
    SELECT * FROM cers_esperanza_1920
    UNION ALL
    SELECT * FROM cers_inspire_1920
    UNION ALL
    SELECT * FROM cers_hs_1920
  ),
  
  cers_empower_1819 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEmpower1819')}}
  ),

  cers_esperanza_1819 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersEsperanza1819')}}
  ),

  cers_inspire_1819 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersInspire1819')}}
  ),

  cers_hs_1819 AS (
    SELECT
      FORMAT("%014d", DistrictId) AS DistrictId,
      DistrictName AS TestDistrictName,
      FORMAT("%014d", SchoolId) AS TestSchoolCdsCode,
      SchoolName AS TestSSchoolName,
      CAST(StudentIdentifier AS STRING) AS StateUniqueId,
      FirstName,
      LastOrSurname AS LastSurname,
      DATE(SubmitDateTime) AS TestDate,
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
    FROM {{ source('RawData', 'CersHighSchool1819')}}
  ),

  cers_1819 AS(
    SELECT * FROM cers_empower_1819
    UNION ALL
    SELECT * FROM cers_esperanza_1819
    UNION ALL
    SELECT * FROM cers_inspire_1819
    UNION ALL
    SELECT * FROM cers_hs_1819
  ),

  cers_all AS (
    SELECT * FROM cers_2122
    UNION ALL
    SELECT * FROM cers_2021
    UNION ALL
    SELECT * FROM cers_1920
    UNION ALL
    SELECT * FROM cers_1819    
  ),

  final AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      c.*
    FROM cers_all AS c
    LEFT JOIN assessment_ids AS a
    ON CONCAT(c.Subject, ' ', c.AssessmentSubType) = a.SubjectAssessmentSubType
  )

SELECT * FROM final
