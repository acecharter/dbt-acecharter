WITH cers_update_dates AS (
  SELECT
    CASE
      WHEN TableName = 'CersEmpower' THEN 'ACE Empower Academy'
      WHEN TableName = 'CersEsperanza' THEN 'ACE Esperanza Middle'
      WHEN TableName = 'CersInspire' THEN 'ACE Inspire Academy'
    END AS SchoolName,
    DateTableLastUpdated
  FROM {{ source('GoogleSheetData', 'ManuallyMaintainedFilesTracker')}}
),
  
cers AS (
  SELECT
    'ACE Empower Academy' AS SchoolNameEnrolled,
    CAST(DistrictId AS STRING) AS DistrictId,
    DistrictName AS DistrictWhereTested,
    CAST(SchoolId AS STRING) AS StateCdsCode,
    SchoolName AS SchoolWhereTestedName,
    CAST(StudentIdentifier AS STRING) AS StateUniqueId,
    FirstName,
    LastOrSurname AS LastName,
    DATE(SubmitDateTime) AS TestDate,
    SchoolYear,
    TestSessionId,
    AssessmentType,
    AssessmentSubType,
    AssessmentName,
    Subject,
    GradeLevelWhenAssessed,
    Completeness,
    ScaleScoreAchievementLevel,
    ScaleScore
  FROM {{ source('GoogleSheetData', 'CersEmpower')}}

  UNION ALL
  SELECT
    'ACE Esperanza Middle' AS SchoolNameEnrolled,
    CAST(DistrictId AS STRING) AS DistrictId,
    DistrictName AS DistrictWhereTested,
    CAST(SchoolId AS STRING) AS StateCdsCode,
    SchoolName AS SchoolWhereTestedName,
    CAST(StudentIdentifier AS STRING) AS StateUniqueId,
    FirstName,
    LastOrSurname AS LastName,
    DATE(SubmitDateTime) AS TestDate,
    SchoolYear,
    TestSessionId,
    AssessmentType,
    AssessmentSubType,
    AssessmentName,
    Subject,
    GradeLevelWhenAssessed,
    Completeness,
    ScaleScoreAchievementLevel,
    ScaleScore
  FROM {{ source('GoogleSheetData', 'CersEsperanza')}}
  
  UNION ALL
  SELECT
    'ACE Inspire Academy' AS SchoolNameEnrolled,
    CAST(DistrictId AS STRING) AS DistrictId,
    DistrictName AS DistrictWhereTested,
    CAST(SchoolId AS STRING) AS StateCdsCode,
    SchoolName AS SchoolWhereTestedName,
    CAST(StudentIdentifier AS STRING) AS StateUniqueId,
    FirstName,
    LastOrSurname AS LastName,
    DATE(SubmitDateTime) AS TestDate,
    SchoolYear,
    TestSessionId,
    AssessmentType,
    AssessmentSubType,
    AssessmentName,
    Subject,
    GradeLevelWhenAssessed,
    Completeness,
    ScaleScoreAchievementLevel,
    ScaleScore
  FROM {{ source('GoogleSheetData', 'CersInspire')}}
)

SELECT
  c.*,
  d.DateTableLastUpdated AS CersExtractDate
FROM cers AS c
LEFT JOIN cers_update_dates AS d
on c.SchoolNameEnrolled = d.SchoolName
