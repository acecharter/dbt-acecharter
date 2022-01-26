WITH empower AS (
  SELECT
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Empower')}}
),

esperanza AS (
  SELECT
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Esperanza')}}
),

inspire AS (
  SELECT
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021Inspire')}}
),

hs AS (
  SELECT
    RecordType,
    SSID,
    GradeAssessed,
    CAST(Attemptedness AS STRING) AS Attemptedness,
    ScoreStatus,
    CAST(IncludeIndicator AS STRING) AS IncludeIndicator,
    LexileorQuantileMeasure,
    ScaleScore,
    AchievementLevels
  FROM {{ source('RawData', 'TomsCaasppEnrolled2021HighSchool')}}
),

final AS (
    SELECT * FROM empower
    UNION All
    SELECT * FROM esperanza
    UNION All
    SELECT * FROM inspire
    UNION All
    SELECT * FROM hs
)

SELECT
  RecordType,
  CASE
    WHEN RecordType = 1 THEN 'SB ELA'
    WHEN RecordType = 2 THEN 'SB Math'
    WHEN RecordType = 3 THEN 'CAA ELA'
    WHEN RecordType = 4 THEN 'CAA Math'
    WHEN RecordType = 6 THEN 'CAST'
    WHEN RecordType = 9 THEN 'CSA'
  END AS TestName,
  CAST(SSID AS STRING) AS SSID,
  GradeAssessed,
  CASE
    WHEN Attemptedness = 'false' THEN 'N'
    WHEN Attemptedness = 'true' THEN 'Y'
    ELSE Attemptedness
  END AS Attemptedness,
  ScoreStatus,
  CASE
    WHEN IncludeIndicator = 'false' THEN 'N'
    WHEN IncludeIndicator = 'true' THEN 'Y'
    ELSE IncludeIndicator
  END AS IncludeIndicator,
  LexileorQuantileMeasure,
  ScaleScore,
  AchievementLevels
FROM final